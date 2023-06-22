package utils

object createtables {

  val stg_dispatch=
    """CREATE TABLE IF NOT EXISTS dispatch(
      pan string,
      card_type	string,
      party_id	string,
      sort_code	string,
      account_no	string,
      event_ts	date,
      event_priority	string,
      notif_code	string,
      notif_sub_code	string,
      notif_narr	string,
      notif_type	string,
      template_id	string,
      a_enttyp	string,
      edh_ingest_ts	date,
      edh_ingest_delete_flag	string,
      edh_source_extract_ts	string
      )
      PARTITIONED BY (
      ingestion_year string,
      ingestion_month	string,
      ingestion_day	string)
      stored as PARQUET; """
  val stg_transactions =
    """ CREATE TABLE IF NOT EXISTS transactions(
    txn_class	string,
    txn_code	string,
    cbs_acc_num	string,
    acc_srt_cd	string,
    crd_num_issue_num	string)
    PARTITIONED BY (
    trn_date string)
    stored as PARQUET; """

  val debit_card_activation_insights =
    """ CREATE TABLE IF NOT EXISTS debit_card_activation_insights(
     acct_no	string,
     sort_code	string,
     pan	string,
     party_id	string,
     brand	string,
     reminder_group	string,
     card_disptach_date	date)
     PARTITIONED BY (
     pipeline_execution_date date
     )
    """


}
