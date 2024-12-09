use aws_config::BehaviorVersion;
use aws_smithy_types_convert::date_time::DateTimeExt;
use bnaclient::types::{
    builder::{self},
    AnalysisPatch, BnaPost, BnaSummary, City, CityPost, CoreServices, Country, Infrastructure,
    Opportunity, People, Recreation, Retail, StateMachineId, Step, Transit,
};
use bnacore::aws::get_aws_parameter_value;
use bnalambdas::{create_service_account_bna_client, AnalysisParameters, Context, Fargate, AWSS3};
use csv::ReaderBuilder;
use heck::ToTitleCase;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde::Deserialize;
use simple_error::SimpleError;
use std::{collections::HashMap, io::Write, str::FromStr};
use tracing::info;
use uuid::Uuid;

const OVERALL_SCORES_COUNT: usize = 23;
const FARGATE_COST_PER_SEC: Decimal = dec!(0.00228333333333);

#[derive(Deserialize)]
struct TaskInput {
    analysis_parameters: AnalysisParameters,
    aws_s3: AWSS3,
    context: Context,
    fargate: Fargate,
}

#[derive(Deserialize, Clone)]
struct OverallScore {
    pub score_id: String,
    pub score_normalized: Option<f64>,
}

#[derive(Deserialize)]
struct OverallScores(HashMap<String, OverallScore>);

impl OverallScores {
    /// Create an empty OverallScores.
    pub fn new() -> Self {
        OverallScores(HashMap::with_capacity(OVERALL_SCORES_COUNT))
    }

    /// Retrieve an OverallScore item by id.
    fn get_overall_score(&self, score_id: &str) -> Option<OverallScore> {
        self.0.get(score_id).cloned()
    }

    /// Retrieve the normalized score of an OverallScore item by id.
    fn get_normalized_score(&self, score_id: &str) -> Option<f64> {
        self.get_overall_score(score_id)
            .and_then(|s| s.score_normalized)
    }
}

async fn function_handler(event: LambdaEvent<TaskInput>) -> Result<(), Error> {
    // Read the task inputs.
    info!("Reading input...");
    let analysis_parameters = &event.payload.analysis_parameters;
    let aws_s3 = &event.payload.aws_s3;
    let state_machine_context = &event.payload.context;
    let state_machine_id = state_machine_context.id;
    let fargate = &event.payload.fargate;

    info!("Retrieve secrets and parameters...");
    // Retrieve API hostname.
    let api_hostname = get_aws_parameter_value("BNA_API_HOSTNAME").await?;

    // Retrieve bna_bucket name.
    let bna_bucket = get_aws_parameter_value("BNA_BUCKET").await?;

    // Create an authenticated BNA client.
    let client_authd = create_service_account_bna_client(&api_hostname).await?;

    // Prepare the AWS configuration.
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;

    // Configure the S3 client.
    info!("Configure the S3 client...");
    let s3_client = aws_sdk_s3::Client::new(&config);

    // Download the CSV file with the results.
    let scores_csv = format!(
        "{}/neighborhood_overall_scores.csv",
        aws_s3.destination.clone()
    );
    info!(
        "Download the CSV file with the results from {}...",
        scores_csv
    );
    let buffer = fetch_s3_object_as_bytes(&s3_client, &bna_bucket, &scores_csv).await?;

    // Parse the results.
    info!("Parse the results...");
    let overall_scores = parse_overall_scores(buffer.as_slice())?;

    // Query city.
    info!("Check for existing city...");
    let country = &analysis_parameters.country;
    let region = match &analysis_parameters.region {
        Some(region) => region,
        None => country,
    };
    let name = &analysis_parameters.city;
    let city_id = get_or_create_city(&client_authd, country, region, name).await?;

    // Convert the overall scores to a BNAPost struct.
    let version = aws_s3.get_version();
    let rating_post = scores_to_bnapost(overall_scores, version, city_id);

    // Post a new entry via the API.
    info!("Post a new BNA entry via the API...");
    info!("New entry: {:?}", &rating_post);
    client_authd.post_ratings().body(rating_post).send().await?;

    // TODO: Patch city census.

    // Configure the ECS client.
    info!("Configure the ECS client...");
    let ecs_client = aws_sdk_ecs::Client::new(&config);

    // Compute the time it took to run the fargate task.
    info!("describing fargate task {}", fargate.task_arn);
    let describe_tasks = ecs_client
        .describe_tasks()
        .cluster(fargate.ecs_cluster_arn.clone())
        .tasks(fargate.task_arn.clone())
        .send()
        .await?;
    let task_info = describe_tasks.tasks().first().unwrap();
    let started_at = task_info
        .started_at()
        .expect("the task must have started at this point");
    let stopped_at = task_info
        .started_at()
        .expect("the task must have stopped at this point");
    let fargate_time = FargateTime::new(*started_at, *stopped_at);

    // Compute the price.
    let cost = fargate_time.cost(FARGATE_COST_PER_SEC);
    info!(cost = ?cost);

    // Update the pipeline status.
    info!("updating pipeline...");
    let start_time = started_at.to_chrono_utc().ok();
    let end_time = stopped_at.to_chrono_utc().ok();
    let _r = client_authd
        .patch_analysis()
        .analysis_id(StateMachineId(state_machine_id))
        .body(
            AnalysisPatch::builder()
                .cost(cost.to_f64().expect("no overflow"))
                .start_time(start_time)
                .end_time(end_time)
                .step(Step::Setup),
        )
        .send()
        .await?;

    Ok(())
}

fn parse_overall_scores(data: &[u8]) -> Result<OverallScores, Error> {
    let mut overall_scores = OverallScores::new();
    let mut rdr = ReaderBuilder::new().flexible(true).from_reader(data);
    for result in rdr.deserialize() {
        let score: OverallScore = result?;
        overall_scores.0.insert(score.score_id.clone(), score);
    }
    Ok(overall_scores)
}

/// Get a city or create it if it does not exist.
async fn get_or_create_city(
    client: &bnaclient::Client,
    country: &str,
    region: &str,
    name: &str,
) -> Result<Uuid, Error> {
    let normalized_country = Country::from_str(country.to_title_case().as_str())?;
    let response = client
        .get_city()
        .country(normalized_country)
        .region(region.to_title_case())
        .name(name.to_title_case())
        .send()
        .await?;
    let city: Option<City> = match response.status().as_u16() {
        x if x < 400 => Some(response.into_inner()),
        404 => None,
        _ => {
            return Err(Box::new(SimpleError::new(format!(
                "cannot retrieve city: {} {}",
                response.status(),
                response.status().as_str()
            ))))
        }
    };
    info!("City: {:#?}", city);
    dbg!(&city);

    let city_id: Uuid;
    if let Some(city) = city {
        info!("The city exists, update the population...");
        city_id = city.id;
    } else {
        info!("Create a new city...");
        // Create the city.
        let c = CityPost::builder()
            .country(normalized_country)
            .state(Some(region.to_title_case()))
            .name(name.to_title_case());
        let city = client.post_city().body(c).send().await?;
        city_id = city.id;
    }

    Ok(city_id)
}

struct FargateTime {
    started_at: aws_sdk_s3::primitives::DateTime,
    stopped_at: aws_sdk_s3::primitives::DateTime,
}

impl FargateTime {
    pub fn new(
        started_at: aws_sdk_s3::primitives::DateTime,
        stopped_at: aws_sdk_s3::primitives::DateTime,
    ) -> Self {
        Self {
            started_at,
            stopped_at,
        }
    }

    /// Returns the duration of the the run in seconds.
    pub fn elapsed(&self) -> i64 {
        (self.started_at.secs() - self.stopped_at.secs()).abs()
    }

    /// Compute the price of a Fargate run.
    pub fn cost(&self, price_per_sec: Decimal) -> Decimal {
        let elapsed_decimal: Decimal = self.elapsed().into();
        elapsed_decimal
            .checked_mul(price_per_sec)
            .expect("no overflow")
    }
}

fn scores_to_bnapost(
    overall_scores: OverallScores,
    version: String,
    city_id: Uuid,
) -> builder::BnaPost {
    BnaPost::builder()
        .core_services(
            CoreServices::builder()
                .dentists(overall_scores.get_normalized_score("core_services_dentists"))
                .doctors(overall_scores.get_normalized_score("core_services_doctors"))
                .grocery(overall_scores.get_normalized_score("core_services_grocery"))
                .hospitals(overall_scores.get_normalized_score("core_services_hospitals"))
                .pharmacies(overall_scores.get_normalized_score("core_services_pharmacies"))
                .dentists(overall_scores.get_normalized_score("core_services_social_services"))
                .score(
                    overall_scores
                        .get_normalized_score("core_services")
                        .unwrap_or_default(),
                ),
        )
        .infrastructure(
            Infrastructure::builder()
                .high_stress_miles(overall_scores.get_normalized_score("total_miles_high_stress"))
                .low_stress_miles(overall_scores.get_normalized_score("total_miles_low_stress")),
        )
        .opportunity(
            Opportunity::builder()
                .employment(overall_scores.get_normalized_score("opportunity_employment"))
                .higher_education(
                    overall_scores.get_normalized_score("opportunity_higher_education"),
                )
                .k12_education(overall_scores.get_normalized_score("opportunity_k12_education"))
                .technical_vocational_college(
                    overall_scores.get_normalized_score("opportunity_technical_vocational_college"),
                )
                .score(
                    overall_scores
                        .get_normalized_score("opportunity")
                        .unwrap_or_default(),
                ),
        )
        .people(People::builder().score(overall_scores.get_normalized_score("people")))
        .recreation(
            Recreation::builder()
                .community_centers(
                    overall_scores.get_normalized_score("recreation_community_centers"),
                )
                .parks(overall_scores.get_normalized_score("recreation_parks"))
                .recreation_trails(overall_scores.get_normalized_score("recreation_trails"))
                .score(
                    overall_scores
                        .get_normalized_score("recreation")
                        .unwrap_or_default(),
                ),
        )
        .retail(Retail::builder().score(overall_scores.get_normalized_score("retail")))
        .summary(
            BnaSummary::builder()
                .rating_id(Uuid::new_v4())
                .city_id(city_id)
                .version(version)
                .score(0.0),
        )
        .transit(Transit::builder().score(overall_scores.get_normalized_score("transit")))
}

async fn fetch_s3_object_as_bytes(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> std::io::Result<Vec<u8>> {
    let mut object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("AWS S3 operation error: {e}"),
            )
        })?;
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(bytes) = object.body.try_next().await? {
        buffer.write_all(&bytes)?;
    }
    Ok(buffer)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await.map_err(|e| {
        info!("{e}");
        e
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::primitives::DateTime;
    use test_log::test;

    #[test]
    fn test_input_deserialization() {
        let json_input = r#"{
          "analysis_parameters": {
            "country": "usa",
            "city": "santa rosa",
            "region": "new mexico",
            "fips_code": "3570670"
          },
          "receipt_handle": "AQEBFo+wTTIZdCvaF2KtZN4ZolAGKeKVGSAhQ7BTA9MUirBT/8mprrHIOg8LuWi3LK9Lu1oFDd5GqVmzExGeHlVbmRA3HWd+vy11b1N4qVeHywvUJJT5/G/GVG2jimkHDa31893N0k2HIm2USSsN6Bqw0JI57ac0ymUWJxzkN9/yJQQXg2dmnNn3YuouzQTGpOJnMjv9UnZaHGVjZXV30IWjs9VzUZd9Wnl721B99pF9t1FUeYnAxShtNUZKzbfbNmSmwtKoE+SwohFL0k84cYkJUjgdXw9yEoT2+zEqeGWtU/oSGmbLorPWIiVYubPcwni1Q9KZROUDvBX7sPDwUeYxxhw9SBxz3y4Tg5hH7X99D4tDXbnRJR1v/0aBAs9h/ohfcEjoYmHdYqRL9r2t33SwYg==",
          "context": {
            "Execution": {
              "Id": "arn:aws:states:us-west-2:863246263227:execution:brokenspoke-analyzer:fd34f1d1-8009-44f1-9111-d3a2daf8a8fe",
              "Name": "fd34f1d1-8009-44f1-9111-d3a2daf8a8fe",
              "RoleArn": "arn:aws:iam::863246263227:role/BNAPipelineLambdaExecution",
              "StartTime": "+002024-04-11T03:05:31.843000000Z"
            },
            "State": {
              "EnteredTime": "+002024-04-11T03:05:32.059000000Z",
              "Name": "BNAContext"
            },
            "StateMachine": {
              "Id": "arn:aws:states:us-west-2:863246263227:stateMachine:brokenspoke-analyzer",
              "Name": "brokenspoke-analyzer"
            },
            "Id": "9ff90cac-0cf5-4923-897f-4416df5e7328"
          },
          "aws_s3": {
            "destination": "usa/new mexico/santa rosa/23.12.4"
          },
          "fargate": {
            "ecs_cluster_arn": "arn:aws:ecs:us-west-2:863246263227:cluster/bna",
            "task_arn": "arn:aws:ecs:us-west-2:863246263227:task/bna/681690ef8bbb446a93e1324f113e75f0",
            "last_status": "STOPPED"
          }
        }"#;
        let _deserialized = serde_json::from_str::<TaskInput>(json_input).unwrap();
    }

    #[test]
    fn test_parse_overallscores() {
        let data = r#"id,score_id,score_original,score_normalized,human_explanation
1,people,0.1917,19.1700,"On average, census blocks in the neighborhood received this population score."
2,opportunity_employment,0.0826,8.2600,"On average, census blocks in the neighborhood received this employment score."
3,opportunity_k12_education,0.0831,8.3100,"On average, census blocks in the neighborhood received this K12 schools score."
4,opportunity_technical_vocational_college,0.0000,0.0000,"On average, census blocks in the neighborhood received this tech/vocational colleges score."
5,opportunity_higher_education,0.0000,0.0000,"On average, census blocks in the neighborhood received this universities score."
6,opportunity,0.0829,8.2900,
7,core_services_doctors,0.0000,0.0000,"On average, census blocks in the neighborhood received this doctors score."
8,core_services_dentists,0.0000,0.0000,"On average, census blocks in the neighborhood received this dentists score."
9,core_services_hospitals,0.0518,5.1800,"On average, census blocks in the neighborhood received this hospital score."
10,core_services_pharmacies,0.0000,0.0000,"On average, census blocks in the neighborhood received this pharmacies score."
11,core_services_grocery,0.0169,1.6900,"On average, census blocks in the neighborhood received this grocery score."
12,core_services_social_services,0.0000,0.0000,"On average, census blocks in the neighborhood received this social services score."
13,core_services,0.0324,3.2400,
14,retail,0.0000,0.0000,"On average, census blocks in the neighborhood received this retail score."
15,recreation_parks,0.0713,7.1300,"On average, census blocks in the neighborhood received this parks score."
16,recreation_trails,0.0000,0.0000,"On average, census blocks in the neighborhood received this trails score."
17,recreation_community_centers,0.0000,0.0000,"On average, census blocks in the neighborhood received this community centers score."
18,recreation,0.0713,7.1300,
19,transit,0.0000,0.0000,"On average, census blocks in the neighborhood received this transit score."
20,overall_score,0.0893,8.9300,
21,population_total,2960.0000,,Total population of boundary
22,total_miles_low_stress,9.3090,9.3000,Total low-stress miles
23,total_miles_high_stress,64.5092,64.5000,Total high-stress miles"#;
        let _scores = parse_overall_scores(data.as_bytes()).unwrap();
    }

    // #[test]
    // fn test_post() {
    //     let data = r#"id,score_id,score_original,score_normalized,human_explanation
    // 1,people,0.1917,19.1700,"On average, census blocks in the neighborhood received this population score."
    // 2,opportunity_employment,0.0826,8.2600,"On average, census blocks in the neighborhood received this employment score."
    // 3,opportunity_k12_education,0.0831,8.3100,"On average, census blocks in the neighborhood received this K12 schools score."
    // 4,opportunity_technical_vocational_college,0.0000,0.0000,"On average, census blocks in the neighborhood received this tech/vocational colleges score."
    // 5,opportunity_higher_education,0.0000,0.0000,"On average, census blocks in the neighborhood received this universities score."
    // 6,opportunity,0.0829,8.2900,
    // 7,core_services_doctors,0.0000,0.0000,"On average, census blocks in the neighborhood received this doctors score."
    // 8,core_services_dentists,0.0000,0.0000,"On average, census blocks in the neighborhood received this dentists score."
    // 9,core_services_hospitals,0.0518,5.1800,"On average, census blocks in the neighborhood received this hospital score."
    // 10,core_services_pharmacies,0.0000,0.0000,"On average, census blocks in the neighborhood received this pharmacies score."
    // 11,core_services_grocery,0.0169,1.6900,"On average, census blocks in the neighborhood received this grocery score."
    // 12,core_services_social_services,0.0000,0.0000,"On average, census blocks in the neighborhood received this social services score."
    // 13,core_services,0.0324,3.2400,
    // 14,retail,0.0000,0.0000,"On average, census blocks in the neighborhood received this retail score."
    // 15,recreation_parks,0.0713,7.1300,"On average, census blocks in the neighborhood received this parks score."
    // 16,recreation_trails,0.0000,0.0000,"On average, census blocks in the neighborhood received this trails score."
    // 17,recreation_community_centers,0.0000,0.0000,"On average, census blocks in the neighborhood received this community centers score."
    // 18,recreation,0.0713,7.1300,
    // 19,transit,0.0000,0.0000,"On average, census blocks in the neighborhood received this transit score."
    // 20,overall_score,0.0893,8.9300,
    // 21,population_total,2960.0000,,Total population of boundary
    // 22,total_miles_low_stress,9.3090,9.3000,Total low-stress miles
    // 23,total_miles_high_stress,64.5092,64.5000,Total high-stress miles"#;
    //     let overall_scores = parse_overall_scores(data.as_bytes()).unwrap();

    //     // Convert the overall scores to a BNAPost struct.
    //     let version = String::from("24.05");
    //     let city_id = Uuid::new_v4();
    //     let bna_post = scores_to_bnapost(overall_scores, version, city_id);
    //     dbg!(&bna_post);
    //     let s = serde_json::to_string(&bna_post);
    //     dbg!(s);

    // info!("Retrieve secrets and parameters...");
    // // Retrieve API hostname.
    // let api_hostname = String::from("https://api.peopleforbikes.xyz");

    // let auth = AuthResponse {
    //     access_token: String::from("")
    //     expires_in: 3600,
    //     token_type: String::from("Bearer"),
    // };

    // // Prepare API URLs.
    // let bnas_url = format!("{api_hostname}/bnas");

    // // Post a new entry via the API.
    // info!("Post a new entry via the API...");
    // Client::new()
    //     .post(bnas_url)
    //     .bearer_auth(auth.access_token.clone())
    //     .json(&bna_post)
    //     .send()
    //     .unwrap()
    //     .error_for_status()
    //     .unwrap();
    // }

    // #[tokio::test]
    // async fn test_fetch_s3_object() {
    //     let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    //     let client = aws_sdk_s3::Client::new(&config);
    //     let buffer = fetch_s3_object_as_bytes(
    //         client,
    //         "brokenspoke-analyzer",
    //         "spain/valencia/valencia/24.4/neighborhood_overall_scores.csv",
    //     )
    //     .await
    //     .unwrap();
    //     assert!(buffer.len() > 0)
    // }

    // #[test]
    // fn test_post_cities() {
    //     let country = String::from("United States");
    //     let region = String::from("New Mexico");
    //     let name = String::from("Santa Rosa");

    //     let api_hostname = "https://api.peopleforbikes.xyz";
    //     let cities_url = format!("{api_hostname}/cities");
    //     let get_cities_url = format!("{cities_url}/{country}/{region}/{name}");

    //     let auth = AuthResponse {
    //         access_token: String::from(""),
    //         expires_in: 3600,
    //         token_type: String::from("Bearer"),
    //     };
    //     // Create the city.
    //     let c = City {
    //         country: country.clone(),
    //         state: region.clone(),
    //         name: name.clone(),
    //         ..Default::default()
    //     };
    //     let client = Client::new();
    //     let r = client
    //         .post(cities_url)
    //         .bearer_auth(auth.access_token.clone())
    //         .json(&c)
    //         .send()
    //         .unwrap()
    //         .error_for_status()
    //         .unwrap();
    //     dbg!(r);
    // }

    // #[tokio::test]
    // async fn test_describe_task() {
    //     let cluster_arn = "arn:aws:ecs:us-west-2:863246263227:cluster/bna";
    //     let fargate_task_arn =
    //         "arn:aws:ecs:us-west-2:863246263227:task/bna/6b7ec08ae2084efb8482173632ecfe6e";
    //     let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    //     let ecs_client = aws_sdk_ecs::Client::new(&config);
    //     let describe_tasks = ecs_client
    //         .describe_tasks()
    //         .cluster(cluster_arn)
    //         .tasks(fargate_task_arn)
    //         .send()
    //         .await
    //         .unwrap();
    //     let task_info = describe_tasks.tasks().first().unwrap();
    //     let started_at = task_info
    //         .started_at()
    //         .expect("the task must have started at this point");
    //     let stopped_at = task_info
    //         .started_at()
    //         .expect("the task must have stopped at this point");
    //     let started_secs = started_at.secs();
    //     let stopped_secs = stopped_at.secs();
    //     let elapsed = (stopped_secs - started_secs) / 1000;
    //     dbg!(task_info);
    //     dbg!(elapsed);
    // }

    #[test]
    fn test_fargate_elapsed() {
        let started_at = DateTime::from_str(
            "1000-01-02T01:23:10.0Z",
            aws_sdk_s3::primitives::DateTimeFormat::DateTime,
        )
        .unwrap();
        let stopped_at = DateTime::from_str(
            "1000-01-02T01:23:20.0Z",
            aws_sdk_s3::primitives::DateTimeFormat::DateTime,
        )
        .unwrap();

        let fargate_time = FargateTime::new(started_at, stopped_at);
        let elapsed = fargate_time.elapsed();
        assert_eq!(elapsed, 10_i64);
    }

    #[test]
    fn test_fargate_cost() {
        let started_at = DateTime::from_str(
            "1000-01-02T01:23:10.0Z",
            aws_sdk_s3::primitives::DateTimeFormat::DateTime,
        )
        .unwrap();
        let stopped_at = DateTime::from_str(
            "1000-01-02T01:23:20.0Z",
            aws_sdk_s3::primitives::DateTimeFormat::DateTime,
        )
        .unwrap();

        let fargate_time = FargateTime::new(started_at, stopped_at);
        assert_eq!(fargate_time.elapsed(), 10);
        assert_eq!(
            fargate_time.cost(FARGATE_COST_PER_SEC),
            dec!(0.0228333333333)
        );
    }

    // #[tokio::test]
    // async fn test_handler() {
    //     let json_input = r#"{
    //     "analysis_parameters": {
    //       "country": "usa",
    //       "city": "santa rosa",
    //       "region": "new mexico",
    //       "fips_code": "3570670"
    //     },
    //     "receipt_handle": "AQEBFo+wTTIZdCvaF2KtZN4ZolAGKeKVGSAhQ7BTA9MUirBT/8mprrHIOg8LuWi3LK9Lu1oFDd5GqVmzExGeHlVbmRA3HWd+vy11b1N4qVeHywvUJJT5/G/GVG2jimkHDa31893N0k2HIm2USSsN6Bqw0JI57ac0ymUWJxzkN9/yJQQXg2dmnNn3YuouzQTGpOJnMjv9UnZaHGVjZXV30IWjs9VzUZd9Wnl721B99pF9t1FUeYnAxShtNUZKzbfbNmSmwtKoE+SwohFL0k84cYkJUjgdXw9yEoT2+zEqeGWtU/oSGmbLorPWIiVYubPcwni1Q9KZROUDvBX7sPDwUeYxxhw9SBxz3y4Tg5hH7X99D4tDXbnRJR1v/0aBAs9h/ohfcEjoYmHdYqRL9r2t33SwYg==",
    //     "context": {
    //       "Execution": {
    //         "Id": "arn:aws:states:us-west-2:863246263227:execution:brokenspoke-analyzer:fd34f1d1-8009-44f1-9111-d3a2daf8a8fe",
    //         "Name": "fd34f1d1-8009-44f1-9111-d3a2daf8a8fe",
    //         "RoleArn": "arn:aws:iam::863246263227:role/BNAPipelineLambdaExecution",
    //         "StartTime": "+002024-04-11T03:05:31.843000000Z"
    //       },
    //       "State": {
    //         "EnteredTime": "+002024-04-11T03:05:32.059000000Z",
    //         "Name": "BNAContext"
    //       },
    //       "StateMachine": {
    //         "Id": "arn:aws:states:us-west-2:863246263227:stateMachine:brokenspoke-analyzer",
    //         "Name": "brokenspoke-analyzer"
    //       },
    //       "Id": "9ff90cac-0cf5-4923-897f-4416df5e7328"
    //     },
    //     "aws_s3": {
    //       "destination": "usa/new mexico/santa rosa/23.12.4"
    //     },
    //     "fargate": {
    //       "ecs_cluster_arn": "arn:aws:ecs:us-west-2:863246263227:cluster/bna",
    //       "task_arn": "arn:aws:ecs:us-west-2:863246263227:task/bna/681690ef8bbb446a93e1324f113e75f0",
    //       "last_status": "STOPPED"
    //     }
    //   }"#;
    //     let payload = serde_json::from_str::<TaskInput>(json_input).unwrap();
    //     let mut context = lambda_runtime::Context::default();
    //     context.request_id = "ID".to_string();

    //     env::set_var("BNA_API_HOSTNAME", "http://localhost:3000");
    //     env::set_var("BNA_BUCKET", "bna-analyzer");

    //     let event = LambdaEvent::new(payload, context);
    //     function_handler(event).await.unwrap();
    // }

    // #[test(tokio::test)]
    // async fn test_get_or_create_city() {
    //     let client = bnaclient::Client::new("http://localhost:3000");
    //     let city = get_or_create_city(&client, "United States", "New Mexico", "Santa Rosa")
    //         .await
    //         .unwrap();
    //     dbg!(city);
    // }

    //     #[test(tokio::test)]
    //     async fn test_post_ratings() {
    //         let data = r#"id,score_id,score_original,score_normalized,human_explanation
    // 1,people,0.1917,19.1700,"On average, census blocks in the neighborhood received this population score."
    // 2,opportunity_employment,0.0826,8.2600,"On average, census blocks in the neighborhood received this employment score."
    // 3,opportunity_k12_education,0.0831,8.3100,"On average, census blocks in the neighborhood received this K12 schools score."
    // 4,opportunity_technical_vocational_college,0.0000,0.0000,"On average, census blocks in the neighborhood received this tech/vocational colleges score."
    // 5,opportunity_higher_education,0.0000,0.0000,"On average, census blocks in the neighborhood received this universities score."
    // 6,opportunity,0.0829,8.2900,
    // 7,core_services_doctors,0.0000,0.0000,"On average, census blocks in the neighborhood received this doctors score."
    // 8,core_services_dentists,0.0000,0.0000,"On average, census blocks in the neighborhood received this dentists score."
    // 9,core_services_hospitals,0.0518,5.1800,"On average, census blocks in the neighborhood received this hospital score."
    // 10,core_services_pharmacies,0.0000,0.0000,"On average, census blocks in the neighborhood received this pharmacies score."
    // 11,core_services_grocery,0.0169,1.6900,"On average, census blocks in the neighborhood received this grocery score."
    // 12,core_services_social_services,0.0000,0.0000,"On average, census blocks in the neighborhood received this social services score."
    // 13,core_services,0.0324,3.2400,
    // 14,retail,0.0000,0.0000,"On average, census blocks in the neighborhood received this retail score."
    // 15,recreation_parks,0.0713,7.1300,"On average, census blocks in the neighborhood received this parks score."
    // 16,recreation_trails,0.0000,0.0000,"On average, census blocks in the neighborhood received this trails score."
    // 17,recreation_community_centers,0.0000,0.0000,"On average, census blocks in the neighborhood received this community centers score."
    // 18,recreation,0.0713,7.1300,
    // 19,transit,0.0000,0.0000,"On average, census blocks in the neighborhood received this transit score."
    // 20,overall_score,0.0893,8.9300,
    // 21,population_total,2960.0000,,Total population of boundary
    // 22,total_miles_low_stress,9.3090,9.3000,Total low-stress miles
    // 23,total_miles_high_stress,64.5092,64.5000,Total high-stress miles"#;
    //         let scores = parse_overall_scores(data.as_bytes()).unwrap();
    //         let bna_post_builder = scores_to_bnapost(
    //             scores,
    //             "24.12".to_string(),
    //             "aac22433-37d4-4cb7-b6b8-e7e77cbbcf41"
    //                 .parse::<Uuid>()
    //                 .unwrap(),
    //         );
    //         dbg!(&bna_post_builder);
    //         let bna_post: BnaPost = bna_post_builder.try_into().unwrap();
    //         println!("{}", serde_json::to_string(&bna_post).unwrap());
    //         let client = bnaclient::Client::new("http://localhost:3000");
    //         let response = client.post_ratings().body(bna_post).send().await.unwrap();
    //         dbg!(response);
    //     }

    // #[test(tokio::test)]
    // async fn test_create_pipeline() {
    //     let client = bnaclient::Client::new("http://localhost:3000");
    //     let analysis_post_builder = AnalysisPost::builder()
    //         .state_machine_id(StateMachineId(uuid::Uuid::new_v4()))
    //         .start_time(Utc::now())
    //         .cost(Decimal::new(10345, 3).to_f64().expect("no overflow"))
    //         .step(Step::Setup);
    //     let analysis_post: AnalysisPost = analysis_post_builder.try_into().unwrap();
    //     println!("{}", serde_json::to_string(&analysis_post).unwrap());
    //     let r = client
    //         .post_ratings_analyses()
    //         .body(analysis_post)
    //         .send()
    //         .await
    //         .unwrap();
    //     dbg!(r);
    // }
}
