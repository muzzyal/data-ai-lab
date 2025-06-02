
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_name
  location                    = var.dataset_location
  project                     = var.dataset_project
  description                 = var.dataset_description
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy  = var.delete_contents_on_destroy
}

# apply access permissions
resource "google_bigquery_dataset_iam_binding" "dataset_bq_owner" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = google_bigquery_dataset.dataset.project
  role       = "roles/bigquery.dataOwner"

  members = concat(
    ["serviceAccount:${var.builder_sa_email}"],
    var.extra_owner_members
  )

}

resource "google_bigquery_dataset_iam_binding" "dataset_bq_editor" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = google_bigquery_dataset.dataset.project
  role       = "roles/bigquery.dataEditor"

  members = var.dataset_specific_sa == true ? concat(
    var.dataset_editor_members,
    [google_service_account.dataset_service_account[0].member]
  ) : var.dataset_editor_members
}

resource "google_bigquery_dataset_iam_binding" "dataset_bq_viewer" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  project    = google_bigquery_dataset.dataset.project
  role       = "roles/bigquery.dataViewer"

  members = var.dataset_viewer_members
}
