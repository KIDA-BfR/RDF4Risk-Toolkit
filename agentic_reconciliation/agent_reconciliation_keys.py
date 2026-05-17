"""Shared runtime-state keys for the agent reconciliation backend."""

AGENT_DATAFRAME_STATE_KEY = "agent_reconciliation_df"
AGENT_DATA_SOURCE_MESSAGE_KEY = "agent_reconciliation_source_message"
AGENT_LAST_SOURCE_NAME_KEY = "agent_reconciliation_last_source_name"
AGENT_INPUT_TABLES_KEY = "agent_reconciliation_input_tables"
AGENT_RESULTS_BY_SOURCE_KEY = "agent_reconciliation_results_by_source"
AGENT_SELECTED_SOURCE_KEY = "agent_reconciliation_selected_source"
AGENT_DEFINITIONS_BY_SOURCE_KEY = "agent_reconciliation_definitions_by_source"
AGENT_RUN_MESSAGES_KEY = "agent_reconciliation_run_messages"
AGENT_AVAILABLE_MODELS_BY_PROVIDER_KEY = "agent_reconciliation_available_models_by_provider"
AGENT_MONITORING_STATE_KEY = "agent_reconciliation_monitoring_state"
AGENT_STOP_EVENT_KEY = "agent_reconciliation_stop_event"
AGENT_RUN_CANCEL_EVENT_STATE_KEY = "agent_reconciliation_run_cancel_event"
AGENT_UPLOADED_SOURCE_SIGNATURE_KEY = "agent_reconciliation_uploaded_source_signature"
AGENT_WORKFLOW_CONFIG_STATE_KEY = "agent_workflow_config_json"
AGENT_ACTIVE_STEP_KEY = "agent_reconciliation_active_step"
AGENT_WORKFLOW_COMPONENT_ACTION_NONCE_KEY = "agent_workflow_component_action_nonce"
AGENT_RUN_STATUS_STATE_KEY = "agent_reconciliation_run_status"
AGENT_RUN_THREAD_STATE_KEY = "agent_reconciliation_run_thread"
AGENT_SSSOM_EXPORT_PAYLOAD_KEY = "agent_reconciliation_sssom_export_payload"

ORCID_BASE_URL = "https://orcid.org/"
REASONING_EFFORT_OPTIONS = ["none", "low", "medium", "high", "xhigh"]
STAGE_TO_COMPONENT = {"Setup": "setup", "Run": "run", "Review": "review", "Export": "export"}
COMPONENT_TO_STAGE = {value: key for key, value in STAGE_TO_COMPONENT.items()}
