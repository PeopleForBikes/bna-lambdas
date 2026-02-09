use aws_config::BehaviorVersion;
use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, NetworkConfiguration,
    TaskOverride,
};
use bnaclient::types::{BnaPipelinePatch, BnaPipelinePost, BnaPipelineStep, PipelineStatus};
use bnacore::aws::get_aws_parameter_value;
use bnalambdas::{create_service_account_bna_client, AnalysisParameters, Context, AWSS3};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Deserialize)]
struct TaskInput {
    analysis_parameters: AnalysisParameters,
    aws_s3: AWSS3,
    context: Context,
}

#[derive(Serialize)]
struct TaskOutput {
    ecs_cluster_arn: String,
    task_arn: String,
    last_status: String,
    command: Vec<String>,
}

const FARGATE_MAX_TASK: i32 = 1;
const DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";
const BNA_EXPERIMENTAL: &str = "1";

async fn function_handler(event: LambdaEvent<TaskInput>) -> Result<TaskOutput, Error> {
    // Retrieve API hostname.
    let api_hostname = get_aws_parameter_value("BNA_API_HOSTNAME").await?;

    // Create an authenticated BNA client.
    let client_authd = create_service_account_bna_client(&api_hostname).await?;

    // Read the task inputs.
    let aws_s3 = &event.payload.aws_s3;
    let analysis_parameters = &event.payload.analysis_parameters;
    let state_machine_context = &event.payload.context;
    let state_machine_id = state_machine_context.id;

    // Create a new pipeline entry.
    info!(
        state_machine_name = state_machine_context.execution.name,
        "create a new Brokenspoke pipeline entry",
    );
    let r = client_authd
        .post_pipelines_bna()
        .body(
            BnaPipelinePost::builder()
                .state_machine_id(state_machine_id)
                .sqs_message(serde_json::to_string(analysis_parameters)?)
                .s3_bucket(aws_s3.destination.clone()),
        )
        .send()
        .await?;
    info!("pipeline created: {:#?}", r);

    // Prepare the AWS client.
    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let ecs_client = aws_sdk_ecs::Client::new(&aws_config);

    // Retrieve secrets and parameters.
    info!("retrieving parameters");
    let ecs_cluster_arn = get_aws_parameter_value("BNA_CLUSTER_ARN").await?;
    let vpc_subnets = get_aws_parameter_value("PUBLIC_SUBNETS").await?;
    let task_security_group = get_aws_parameter_value("BNA_TASK_SECURITY_GROUP").await?;
    let task_definition = get_aws_parameter_value("BNA_TASK_DEFINITION").await?;
    let s3_bucket = get_aws_parameter_value("BNA_BUCKET").await?;
    let _cache_directory = get_aws_parameter_value("BNA_CACHE_DIRECTORY").await?;

    // Prepare the command.
    info!("Preparing the container command");
    let mut container_command: Vec<String> = vec![
        "-vv".to_string(),
        "run".to_string(),
        "--no-cache".to_string(),
        // "--cache-dir".to_string(),
        // cache_directory,
        "--with-export".to_string(),
        "s3_custom".to_string(),
        "--s3-bucket".to_string(),
        s3_bucket,
        "--s3-dir".to_string(),
        aws_s3.destination.clone(),
        analysis_parameters.country.clone(),
        analysis_parameters.city.clone(),
    ];
    if analysis_parameters.region.is_some() {
        container_command.push(analysis_parameters.region.clone().unwrap());
        container_command.push(analysis_parameters.fips_code.clone().unwrap());
    };

    info!(container_command = container_command.join(" "));

    // Prepare and run the task.
    info!("preparing the task");
    let container_name = "brokenspoke-analyzer".to_string();
    let container_overrides = ContainerOverride::builder()
        .name(container_name)
        .set_command(Some(container_command.clone()))
        .environment(
            KeyValuePair::builder()
                .name("DATABASE_URL".to_string())
                .value(DATABASE_URL)
                .build(),
        )
        .environment(
            KeyValuePair::builder()
                .name("BNA_EXPERIMENTAL")
                .value(BNA_EXPERIMENTAL)
                .build(),
        )
        .build();
    let task_overrides = TaskOverride::builder()
        .container_overrides(container_overrides)
        .build();
    let mut aws_vpc_configuration_builder = AwsVpcConfiguration::builder()
        .security_groups(task_security_group)
        .assign_public_ip(AssignPublicIp::Enabled);
    for subnet in vpc_subnets.split(',').map(String::from) {
        aws_vpc_configuration_builder = aws_vpc_configuration_builder.subnets(subnet);
    }
    let aws_vpc_configuration = aws_vpc_configuration_builder.build()?;
    let network_configuration = NetworkConfiguration::builder()
        .awsvpc_configuration(aws_vpc_configuration)
        .build();
    let run_task_output = ecs_client
        .run_task()
        .cluster(ecs_cluster_arn)
        .count(FARGATE_MAX_TASK)
        .launch_type(aws_sdk_ecs::types::LaunchType::Fargate)
        .network_configuration(network_configuration)
        .overrides(task_overrides)
        .task_definition(task_definition)
        .send()
        .await?;

    // Prepare the output.
    let task = run_task_output
        .tasks()
        .first()
        .expect("there must be one task");
    let output = TaskOutput {
        ecs_cluster_arn: task.cluster_arn().unwrap().into(),
        task_arn: task.task_arn().unwrap().into(),
        last_status: task.last_status().unwrap().into(),
        command: container_command,
    };

    // Update the pipeline status.
    info!("updating the pipeline status");
    client_authd
        .patch_pipelines_bna()
        .pipeline_id(state_machine_id)
        .body(
            BnaPipelinePatch::builder()
                .fargate_task_arn(task.task_arn().expect("an existing task ARN").to_string())
                .status(PipelineStatus::Processing)
                .step(BnaPipelineStep::Analysis),
        )
        .send()
        .await?;

    Ok(output)
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

    #[test]
    fn test_input_deserialization() {
        let json_input = r#"
        {
          "analysis_parameters": {
            "country": "usa",
            "city": "santa rosa",
            "region": "new mexico",
            "fips_code": "3570670"
          },
          "receipt_handle": "AQEBMtAMiWSYxry6iA8NH0wHUYvOXNLS00piVRqNYWlI5Cs8RRhd21R+5L46DsJgQtbNyrnUATM6Dw70nQoKQ5nFaU3GjK+Aone90aWVAB7DPcYpnUt9uxKdRLdgeNUAAHvBT+K83cJgHwL2ek/fGHPEBCZGN8CV2ZXEDoY2GFfRB51el+4f61YqsIxOEOpgV0djb2D0B/WzS8i8BznanguRn3bT8iz0RXk60hZjp01PN9ljSqjpFwlXM0TLx3tI1RgVYconH2CGnII9qtWz0A4MciKW0vOnKyA70AfUgDPgFFmw6OTwuPeLedCt6lhpYc7fZUGuRAc/Ozz8uAkEI6eTm2yxh1p0OJzXDoqEEaoFgsHHaHOgulmL5QwhZw3z/lBEDii8g4MTZ6UqekkK9dcxew==",
          "context": {
            "Execution": {
              "Id": "arn:aws:states:us-west-2:123456789012:execution:brokenspoke-analyzer:73f24dfc-8978-4d93-a4f7-29d1b0263e4a",
              "Name": "73f24dfc-8978-4d93-a4f7-29d1b0263e4a",
              "RoleArn": "arn:aws:iam::123456789012:role/role",
              "StartTime": "+002024-02-13T00:22:50.787000000Z"
            },
            "State": {
              "EnteredTime": "+002024-02-13T00:22:51.019000000Z",
              "Name": "BNAContext"
            },
            "StateMachine": {
              "Id": "arn:aws:states:us-west-2:123456789012:stateMachine:brokenspoke-analyzer",
              "Name": "brokenspoke-analyzer"
            },
            "Id": "9ff90cac-0cf5-4923-897f-4416df5e7328"
          },
          "aws_s3": {
            "destination": "usa/new mexico/santa rosa/24.05.3"
          }
        }"#;
        let _deserialized = serde_json::from_str::<TaskInput>(json_input).unwrap();
    }

    // #[tokio::test]
    // async fn test_create_pipeline() {
    //     // let auth = AuthResponse {
    //     //     access_token: String::from(""),
    //     //     expires_in: 3600,
    //     //     token_type: String::from("Bearer"),
    //     // };
    //     let client = bnaclient::Client::new("http://localhost:3000");
    //     let r = client
    //         .post_pipelines_bna()
    //         .body(
    //             BnaPipelinePost::builder()
    //                 .state_machine_id(Uuid::new_v4())
    //                 .s3_bucket("/destination".to_string()),
    //         )
    //         .send()
    //         .await;
    //     dbg!(r);
    // }

    // #[tokio::test]
    // async fn test_hanlder() {
    //     let smi = Uuid::new_v4();
    //     let json_input = json!(
    //     {
    //       "analysis_parameters": {
    //         "country": "united states",
    //         "city": "los angeles",
    //         "region": "california",
    //         "fips_code": "644000"
    //       },
    //       "receipt_handle": "AQEBMtAMiWSYxry6iA8NH0wHUYvOXNLS00piVRqNYWlI5Cs8RRhd21R+5L46DsJgQtbNyrnUATM6Dw70nQoKQ5nFaU3GjK+Aone90aWVAB7DPcYpnUt9uxKdRLdgeNUAAHvBT+K83cJgHwL2ek/fGHPEBCZGN8CV2ZXEDoY2GFfRB51el+4f61YqsIxOEOpgV0djb2D0B/WzS8i8BznanguRn3bT8iz0RXk60hZjp01PN9ljSqjpFwlXM0TLx3tI1RgVYconH2CGnII9qtWz0A4MciKW0vOnKyA70AfUgDPgFFmw6OTwuPeLedCt6lhpYc7fZUGuRAc/Ozz8uAkEI6eTm2yxh1p0OJzXDoqEEaoFgsHHaHOgulmL5QwhZw3z/lBEDii8g4MTZ6UqekkK9dcxew==",
    //       "context": {
    //         "Execution": {
    //           "Id": "arn:aws:states:us-west-2:123456789012:execution:brokenspoke-analyzer:73f24dfc-8978-4d93-a4f7-29d1b0263e4a",
    //           "Name": "73f24dfc-8978-4d93-a4f7-29d1b0263e4a",
    //           "RoleArn": "arn:aws:iam::123456789012:role/role",
    //           "StartTime": "+002024-02-13T00:22:50.787000000Z"
    //         },
    //         "State": {
    //           "EnteredTime": "+002024-02-13T00:22:51.019000000Z",
    //           "Name": "BNAContext"
    //         },
    //         "StateMachine": {
    //           "Id": "arn:aws:states:us-west-2:123456789012:stateMachine:brokenspoke-analyzer",
    //           "Name": "brokenspoke-analyzer"
    //         },
    //         "Id":smi
    //       },
    //       "aws_s3": {
    //         "destination": "united states/california/los angeles/24.12.1"
    //       }
    //     });
    //     let payload = serde_json::from_value::<TaskInput>(json_input).unwrap();
    //     let mut context = lambda_runtime::Context::default();
    //     context.request_id = "ID".to_string();
    //     std::env::set_var("BNA_API_HOSTNAME", "http://localhost:3000");
    //     std::env::set_var("BNA_BUCKET", "bna-analyzer");
    //     let event = LambdaEvent::new(payload, context);
    //     function_handler(event).await.unwrap();
    // }
}
