use nautilus_model::types::Price;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StrikeChangeSmoothingState {
    pending_strikes: Option<Vec<Price>>,
    confirmations: u32,
}

impl StrikeChangeSmoothingState {
    pub fn reset(&mut self) {
        self.pending_strikes = None;
        self.confirmations = 0;
    }
}

/// Returns `true` when a strike-window change should be applied now.
///
/// Expiry rolls bypass smoothing. When `confirmations_required` is zero,
/// smoothing is disabled and every strike change applies immediately.
#[must_use]
pub fn should_apply_strike_change(
    current_strikes: &[Price],
    candidate_strikes: &[Price],
    expiry_changed: bool,
    confirmations_required: u32,
    state: &mut StrikeChangeSmoothingState,
) -> bool {
    if expiry_changed || confirmations_required == 0 {
        state.reset();
        return true;
    }

    if current_strikes == candidate_strikes {
        state.reset();
        return true;
    }

    let candidate_key = candidate_strikes.to_vec();
    if state.pending_strikes.as_ref() == Some(&candidate_key) {
        state.confirmations = state.confirmations.saturating_add(1);
    } else {
        state.pending_strikes = Some(candidate_key);
        state.confirmations = 1;
    }

    if state.confirmations >= confirmations_required {
        state.reset();
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoothing_requires_consecutive_confirmations() {
        let current = vec![Price::from("65000"), Price::from("66000")];
        let candidate = vec![Price::from("64000"), Price::from("65000")];
        let mut state = StrikeChangeSmoothingState::default();

        assert!(!should_apply_strike_change(
            &current, &candidate, false, 2, &mut state
        ));
        assert!(should_apply_strike_change(
            &current, &candidate, false, 2, &mut state
        ));
    }

    #[test]
    fn smoothing_resets_when_candidate_changes() {
        let current = vec![Price::from("65000")];
        let first_candidate = vec![Price::from("64000")];
        let second_candidate = vec![Price::from("63000")];
        let mut state = StrikeChangeSmoothingState::default();

        assert!(!should_apply_strike_change(
            &current, &first_candidate, false, 2, &mut state
        ));
        assert!(!should_apply_strike_change(
            &current, &second_candidate, false, 2, &mut state
        ));
        assert!(should_apply_strike_change(
            &current, &second_candidate, false, 2, &mut state
        ));
    }

    #[test]
    fn expiry_roll_bypasses_smoothing() {
        let current = vec![Price::from("65000")];
        let candidate = vec![Price::from("64000")];
        let mut state = StrikeChangeSmoothingState::default();
        state.pending_strikes = Some(candidate.clone());
        state.confirmations = 1;

        assert!(should_apply_strike_change(
            &current, &candidate, true, 3, &mut state
        ));
        assert_eq!(state, StrikeChangeSmoothingState::default());
    }

    #[test]
    fn zero_confirmations_disables_smoothing() {
        let current = vec![Price::from("65000")];
        let candidate = vec![Price::from("64000")];
        let mut state = StrikeChangeSmoothingState::default();

        assert!(should_apply_strike_change(
            &current, &candidate, false, 0, &mut state
        ));
    }
}