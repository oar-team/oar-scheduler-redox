use crate::model::configuration::{Configuration, JobPriority, QuotasAllNbResourcesMode};
use pyo3::exceptions::PyValueError;
use pyo3::types::PyDict;
use pyo3::{prelude::PyAnyMethods, types::PyString, Bound, FromPyObject, IntoPyObject, PyAny, PyErr, PyResult, Python};

impl<'p> IntoPyObject<'p> for &JobPriority {
    type Target = PyString;
    type Output = Bound<'p, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'p>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            JobPriority::Fifo => "FIFO",
            JobPriority::Fairshare => "FAIRSHARE",
            JobPriority::Multifactor => "MULTIFACTOR",
        };
        Ok(PyString::new(py, s))
    }
}
impl<'a> FromPyObject<'a> for JobPriority {
    fn extract_bound(obj: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = obj.extract()?;
        match s.as_str() {
            "FIFO" => Ok(JobPriority::Fifo),
            "FAIRSHARE" => Ok(JobPriority::Fairshare),
            "MULTIFACTOR" => Ok(JobPriority::Multifactor),
            _ => Err(PyErr::new::<PyValueError, _>(format!("Invalid JobPriority: {}", s))),
        }
    }
}

impl<'a> IntoPyObject<'a> for &QuotasAllNbResourcesMode {
    type Target = PyString;
    type Output = Bound<'a, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'a>) -> Result<Self::Output, Self::Error> {
        let s = match self {
            QuotasAllNbResourcesMode::All => "all",
            QuotasAllNbResourcesMode::DefaultNotDead => "default_not_dead",
        };
        Ok(PyString::new(py, s))
    }
}

impl<'a> FromPyObject<'a> for QuotasAllNbResourcesMode {
    fn extract_bound(obj: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = obj.extract()?;
        match s.as_str() {
            "all" => Ok(QuotasAllNbResourcesMode::All),
            "default_not_dead" => Ok(QuotasAllNbResourcesMode::DefaultNotDead),
            _ => Err(PyErr::new::<PyValueError, _>(format!("Invalid QuotasAllNbResourcesMode: {}", s))),
        }
    }
}

impl<'p> IntoPyObject<'p> for &Configuration {
    type Target = PyDict;
    type Output = Bound<'p, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'p>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        dict.set_item("JOB_PRIORITY", (&self.job_priority).into_pyobject(py)?)?;
        dict.set_item("PRIORITY_CONF_FILE", self.priority_conf_file.clone())?;
        dict.set_item("SCHEDULER_JOB_SECURITY_TIME", self.scheduler_job_security_time)?;
        dict.set_item("QUOTAS", PyString::new(py, if self.quotas { "yes" } else { "no" }))?;
        dict.set_item("QUOTAS_CONF_FILE", self.quotas_conf_file.clone())?;
        if let Some(v) = self.quotas_window_time_limit { dict.set_item("QUOTAS_WINDOW_TIME_LIMIT", v)?; }
        dict.set_item("QUOTAS_ALL_NB_RESOURCES_MODE", (&self.quotas_all_nb_resources_mode).into_pyobject(py)?)?;
        dict.set_item("CACHE_ENABLED", PyString::new(py, if self.cache_enabled { "yes" } else { "no" }))?;

        // Optional SCHEDULER_FAIRSHARING_* fields
        if let Some(v) = self.scheduler_fairsharing_window_size { dict.set_item("SCHEDULER_FAIRSHARING_WINDOW_SIZE", v)?; }
        if let Some(v) = &self.scheduler_fairsharing_project_targets { dict.set_item("SCHEDULER_FAIRSHARING_PROJECT_TARGETS", v.clone())?; }
        if let Some(v) = &self.scheduler_fairsharing_user_targets { dict.set_item("SCHEDULER_FAIRSHARING_USER_TARGETS", v.clone())?; }
        if let Some(v) = self.scheduler_fairsharing_coef_project { dict.set_item("SCHEDULER_FAIRSHARING_COEF_PROJECT", v)?; }
        if let Some(v) = self.scheduler_fairsharing_coef_user { dict.set_item("SCHEDULER_FAIRSHARING_COEF_USER", v)?; }
        if let Some(v) = self.scheduler_fairsharing_coef_user_ask { dict.set_item("SCHEDULER_FAIRSHARING_COEF_USER_ASK", v)?; }

        Ok(dict)
    }
}

fn get_opt_str_config(dict: &Bound<PyDict>, key: &str) -> PyResult<Option<String>> {
    if let Some(value) = dict.get_item(key).ok() {
        let value: String = value
            .extract()
            .map_err(|_e| PyErr::new::<PyValueError, _>(format!("Invalid {} configuration entry: should be a String.", key)))?;
        Ok(Some(value.to_string()))
    } else {
        Ok(None)
    }
}
fn get_str_config(dict: &Bound<PyDict>, key: &str) -> PyResult<String> {
    get_opt_str_config(dict, key)?.ok_or_else(|| PyErr::new::<PyValueError, _>(format!("Missing {} configuration entry.", key)))
}
fn get_opt_i64_config(dict: &Bound<PyDict>, key: &str) -> PyResult<Option<i64>> {
    if let Some(value) = dict.get_item(key).ok() {
        if let Ok(str) = value.extract::<String>() {
            return if let Ok(v) = str.parse::<i64>() {
                Ok(Some(v))
            } else {
                Err(PyErr::new::<PyValueError, _>(format!("Invalid {} configuration entry: should be an integer or an integer as a string.", key)))
            };
        }
        let value: i64 = value
            .extract()
            .map_err(|_e| PyErr::new::<PyValueError, _>(format!("Invalid {} configuration entry: should be an integer.", key)))?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}
fn get_i64_config(dict: &Bound<PyDict>, key: &str) -> PyResult<i64> {
    get_opt_i64_config(dict, key)?.ok_or_else(|| PyErr::new::<PyValueError, _>(format!("Missing {} configuration entry.", key)))
}
fn get_opt_f64_config(dict: &Bound<PyDict>, key: &str) -> PyResult<Option<f64>> {
    if let Some(value) = dict.get_item(key).ok() {
        if let Ok(str_v) = value.extract::<String>() {
            return str_v.parse::<f64>().map(Some).map_err(|_| {
                PyErr::new::<PyValueError, _>(format!(
                    "Invalid {} configuration entry: should be a float or a float as a string.",
                    key
                ))
            });
        }
        let value: f64 = value
            .extract()
            .map_err(|_e| PyErr::new::<PyValueError, _>(format!("Invalid {} configuration entry: should be a float.", key)))?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}
fn get_opt_bool_config(dict: &Bound<PyDict>, key: &str) -> PyResult<Option<bool>> {
    if let Some(value) = dict.get_item(key).ok() {
        // check if it is a py boolean
        if let Ok(b) = value.extract::<bool>() {
            return Ok(Some(b));
        }
        let value: String = value.extract().map_err(|_e| {
            PyErr::new::<PyValueError, _>(format!(
                "Invalid {} configuration entry: should be 'yes', 'no', or a Python boolean.",
                key
            ))
        })?;
        match value.as_str() {
            "yes" => Ok(Some(true)),
            "no" => Ok(Some(false)),
            _ => Err(PyErr::new::<PyValueError, _>(format!(
                "Invalid {} configuration entry: should be 'yes', 'no', or a Python boolean.",
                key
            ))),
        }
    } else {
        Ok(None)
    }
}
fn get_bool_config(dict: &Bound<PyDict>, key: &str) -> PyResult<bool> {
    get_opt_bool_config(dict, key)?.ok_or_else(|| PyErr::new::<PyValueError, _>(format!("Missing {} configuration entry.", key)))
}
fn get_opt_any_config<'a, A>(dict: &Bound<'a, PyDict>, key: &str) -> PyResult<Option<A>>
where
    A: FromPyObject<'a>,
{
    if let Some(value) = dict.get_item(key).ok() {
        Ok(Some(value.extract().map_err(|e| {
            PyErr::new::<PyValueError, _>(format!(
                "Invalid {} configuration entry: could not convert to the desired type: {}",
                key, e.to_string()
            ))
        })?))
    } else {
        Err(PyErr::new::<PyValueError, _>(format!("Missing {} configuration entry.", key)))
    }
}
fn get_any_config<'a, A>(dict: &Bound<'a, PyDict>, key: &str) -> PyResult<A>
where
    A: FromPyObject<'a>,
{
    get_opt_any_config(dict, key)?.ok_or_else(|| PyErr::new::<PyValueError, _>(format!("Missing {} configuration entry.", key)))
}

impl<'a> FromPyObject<'a> for Configuration {
    fn extract_bound(obj: &Bound<'a, PyAny>) -> PyResult<Self> {
        let dict: &Bound<'a, PyDict> = obj.downcast()?;
        Ok(Configuration {
            job_priority: get_opt_any_config(&dict, "JOB_PRIORITY")?.unwrap_or(JobPriority::Fifo),
            priority_conf_file: get_opt_str_config(dict, "PRIORITY_CONF_FILE")?,
            scheduler_job_security_time: get_i64_config(dict, "SCHEDULER_JOB_SECURITY_TIME")?,
            quotas: get_bool_config(dict, "QUOTAS")?,
            quotas_conf_file: get_opt_str_config(dict, "QUOTAS_CONF_FILE")?,
            quotas_window_time_limit: get_opt_i64_config(dict, "QUOTAS_WINDOW_TIME_LIMIT")?,
            quotas_all_nb_resources_mode: get_opt_any_config(&dict, "QUOTAS_ALL_NB_RESOURCES_MODE")?.unwrap_or(QuotasAllNbResourcesMode::All),
            cache_enabled: get_opt_bool_config(dict, "CACHE_ENABLED")?.unwrap_or(true),
            scheduler_fairsharing_window_size: get_opt_i64_config(dict, "SCHEDULER_FAIRSHARING_WINDOW_SIZE")?,
            scheduler_fairsharing_project_targets: get_opt_str_config(dict, "SCHEDULER_FAIRSHARING_PROJECT_TARGETS")?,
            scheduler_fairsharing_user_targets: get_opt_str_config(dict, "SCHEDULER_FAIRSHARING_USER_TARGETS")?,
            scheduler_fairsharing_coef_project: get_opt_f64_config(dict, "SCHEDULER_FAIRSHARING_COEF_PROJECT")?,
            scheduler_fairsharing_coef_user: get_opt_f64_config(dict, "SCHEDULER_FAIRSHARING_COEF_USER")?,
            scheduler_fairsharing_coef_user_ask: get_opt_f64_config(dict, "SCHEDULER_FAIRSHARING_COEF_USER_ASK")?,
        })
    }
}
