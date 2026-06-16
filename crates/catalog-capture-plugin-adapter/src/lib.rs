#[derive(Debug, Default)]
pub struct PluginAdapterNotes;

impl PluginAdapterNotes {
    pub const STATUS: &'static str =
        "Treat plugin integration as a deployment shell around a dedicated capture actor, not as the core capture architecture.";

    pub const PLATFORM_NOTE: &'static str =
        "Current Nautilus live plugin support should be treated conservatively and validated per deployment target.";

    pub const DESIGN_INTENT: &'static str =
        "If plugin deployment is chosen, the plugin adapter should forward runtime callbacks into the same capture core and actor-oriented batching model used by non-plugin deployments.";
}
