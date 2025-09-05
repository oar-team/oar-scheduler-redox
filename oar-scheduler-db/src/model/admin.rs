use sea_query::Iden;

#[derive(Iden)]
pub enum AdmissionRules {
    #[iden = "admission_rules"]
    Table,
    #[iden = "id"]
    Id,
    #[iden = "priority"]
    Priority,
    #[iden = "enabled"]
    Enabled,
    #[iden = "rule"]
    Rule,
}

#[derive(Iden)]
pub enum SchemaMeta {
    #[iden = "schema"]
    Table,
    #[iden = "version"]
    Version,
    #[iden = "name"]
    Name,
}
