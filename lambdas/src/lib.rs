use bnacore::aws::{get_aws_parameter_value, get_aws_secrets_value};
use chrono::{DateTime, Utc};
use lambda_runtime::Error;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BROKENSPOKE_ANALYZER_BUCKET: &str = "brokenspoke-analyzer";

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AnalysisParameters {
    pub country: String,
    pub city: String,
    pub region: Option<String>,
    pub fips_code: Option<String>,
}

impl AnalysisParameters {
    /// Create a new AnalysisParameter object.
    pub fn new(
        country: String,
        city: String,
        region: Option<String>,
        fips_code: Option<String>,
    ) -> Self {
        Self {
            country,
            city,
            region,
            fips_code: fips_code.or(Some("0".to_string())),
        }
    }

    /// Create a new simple AnalysisParameter object with only a city and a country.
    pub fn simple(country: String, city: String) -> Self {
        Self::new(country, city, None, None)
    }

    /// Create a new AnalysisParameter object with a city, a country, and a region.
    pub fn with_region(country: String, city: String, region: String) -> Self {
        Self::new(country, city, Some(region), None)
    }

    /// Create a new AnalysisParameter object with a city, a country, a region and a FIPS code.
    pub fn with_fips_code(
        country: String,
        city: String,
        region: String,
        fips_code: String,
    ) -> Self {
        Self::new(country, city, Some(region), Some(fips_code))
    }

    /// Ensure all the parameters are populated appropriately.
    pub fn sanitized(&self) -> Self {
        let region = match &self.region {
            Some(region) => Some(region.clone()),
            None => Some(self.country.clone()),
        };
        let fips_code = match &self.fips_code {
            Some(fips_code) => Some(fips_code.clone()),
            None => Some("0".to_string()),
        };
        Self {
            country: self.country.clone(),
            city: self.city.clone(),
            region,
            fips_code,
        }
    }
}

/// Define Cognito autnetication response.
#[derive(Debug, Deserialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub expires_in: u32,
    pub token_type: String,
}

/// Define Cognito app client credentials.
pub struct AppClientCredentials {
    pub client_id: String,
    pub client_secret: String,
}

/// Retrieve service account credentials.
pub async fn get_service_account_credentials() -> Result<AppClientCredentials, bnacore::Error> {
    const SERVICE_ACCOUNT_CREDENTIALS: &str = "BROKENSPOKE_ANALYZER_SERVICE_ACCOUNT_CREDENTIALS";
    let client_id = get_aws_secrets_value(SERVICE_ACCOUNT_CREDENTIALS, "client_id").await?;
    let client_secret = get_aws_secrets_value(SERVICE_ACCOUNT_CREDENTIALS, "client_secret").await?;
    Ok(AppClientCredentials {
        client_id,
        client_secret,
    })
}

pub async fn authenticate(
    credentials: &AppClientCredentials,
) -> Result<AuthResponse, bnacore::Error> {
    const COGNITO_HOSTNAME: &str = "BNA_COGNITO_HOSTNAME";
    let cognito_hostname = get_aws_parameter_value(COGNITO_HOSTNAME).await?;
    let token_endpoint = format!("{cognito_hostname}/oauth2/token");
    Ok(Client::new()
        .post(token_endpoint)
        .form(&[
            ("grant_type", "client_credentials"),
            ("scope", "service_account/write"),
        ])
        .basic_auth(
            credentials.client_id.clone(),
            Some(credentials.client_secret.clone()),
        )
        .send()?
        .error_for_status()?
        .json::<AuthResponse>()?)
}

pub async fn authenticate_service_account() -> Result<AuthResponse, bnacore::Error> {
    let credentials = get_service_account_credentials().await?;
    authenticate(&credentials).await
}

/// Define a state machine context object.
///
/// https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Context {
    pub execution: Execution,
    pub state: State,
    pub state_machine: StateMachine,
    pub id: Uuid,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Execution {
    pub id: String,
    pub name: String,
    pub role_arn: String,
    pub start_time: DateTime<Utc>,
}

impl Execution {
    /// Parse the execution name into the state machine ID and the scheduled trigger id if available.
    ///
    /// ```
    /// use bnalambdas::Execution;
    /// use chrono::Utc;
    /// use uuid::Uuid;
    ///
    /// let execution = Execution {
    ///   id: "id".to_string(),
    ///   name: "e6aade5a-b343-120b-dbaa-bd916cd99221_04ca18b9-6e0c-1aa5-2c3f-d4b445f840bc".to_string(),
    ///   role_arn: "role".to_string(),
    ///   start_time: Utc::now(),
    /// };
    /// let (state_machine_id, schedule_trigger_id) = execution.ids().unwrap();
    /// assert_eq!(
    ///     state_machine_id,
    ///     Uuid::parse_str("e6aade5a-b343-120b-dbaa-bd916cd99221").unwrap()
    /// );
    /// assert_eq!(
    ///     schedule_trigger_id,
    ///     Some(Uuid::parse_str("04ca18b9-6e0c-1aa5-2c3f-d4b445f840bc").unwrap())
    /// );
    /// ```
    pub fn ids(&self) -> Result<(Uuid, Option<Uuid>), uuid::Error> {
        let mut parts = self.name.split('_');
        let state_machine_id = parts.next().unwrap().parse::<Uuid>()?;
        let scheduled_trigger_id = parts.next().map(|s| s.parse::<Uuid>()).transpose()?;

        Ok((state_machine_id, scheduled_trigger_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct State {
    pub entered_time: DateTime<Utc>,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StateMachine {
    pub id: String,
    pub name: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct AWSS3 {
    pub destination: String,
}

impl AWSS3 {
    /// Returns the get version of this [`AWSS3`].
    pub fn get_version(&self) -> String {
        self.destination
            .split_terminator('/')
            .next_back()
            .expect("the destination field must contain a `/` symbol")
            .to_owned()
    }
}

#[derive(Deserialize, Serialize)]
pub struct Fargate {
    pub ecs_cluster_arn: String,
    pub task_arn: String,
    pub last_status: String,
}

/// Creates and authenticated BNA client.
pub fn create_authenticated_bna_client(base_url: &str, auth: &AuthResponse) -> bnaclient::Client {
    let mut val =
        reqwest::header::HeaderValue::from_str(format!("Bearer {}", auth.access_token).as_str())
            .expect("a valid auth token");
    val.set_sensitive(true);
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, val);
    let client_builder: reqwest::Client = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .build()
        .unwrap();
    bnaclient::Client::new_with_client(base_url, client_builder)
}

/// Creates and authenticated BNA client for the service account.
pub async fn create_service_account_bna_client(base_url: &str) -> Result<bnaclient::Client, Error> {
    let auth = authenticate_service_account()
        .await
        .map_err(|e| format!("cannot authenticate service account: {e}"))?;
    Ok(create_authenticated_bna_client(base_url, &auth))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Uses the example provided in the official Step Function documentation:
    // https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html#contextobject-format
    #[test]
    fn test_serde_context() {
        let raw_json = r#"{
          "Execution": {
              "Id": "arn:aws:states:us-east-1:123456789012:execution:stateMachineName:executionName",
              "Input": {
                 "key": "value"
              },
              "Name": "executionName",
              "RoleArn": "arn:aws:iam::123456789012:role...",
              "StartTime": "2019-03-26T20:14:13.192Z"
          },
          "State": {
              "EnteredTime": "2019-03-26T20:14:13.192Z",
              "Name": "Test",
              "RetryCount": 3
          },
          "StateMachine": {
              "Id": "arn:aws:states:us-east-1:123456789012:stateMachine:stateMachineName",
              "Name": "stateMachineName"
          },
          "Id": "9ff90cac-0cf5-4923-897f-4416df5e7328",
          "Task": {
              "Token": "h7XRiCdLtd/83p1E0dMccoxlzFhglsdkzpK9mBVKZsp7d9yrT1W"
          }
      }"#;
        let deserialized = serde_json::from_str::<Context>(raw_json).unwrap();
        assert_eq!(deserialized.execution.name, "executionName")
    }

    #[test]
    fn test_ids_partial() {
        let name = "e6aade5a-b343-120b-dbaa-bd916cd99221".to_string();
        let deserialized = Execution {
            id: "id".to_string(),
            name: name.clone(),
            role_arn: "role".to_string(),
            start_time: Utc::now(),
        };
        let (state_machine_id, schedule_trigger_id) = deserialized.ids().unwrap();
        assert_eq!(state_machine_id, Uuid::parse_str(&name).unwrap());
        assert_eq!(schedule_trigger_id, None);
    }
}
