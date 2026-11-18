"""Marker `md`: the tier T0 tests of this package once more, against real MotherDuck.

The test functions are the ones of `test_lane.py`, `test_schema.py`, `test_state.py`, `test_alerts.py`,
`test_from_apply.py` and `test_fix_round.py`, unchanged. Only the fixture `dest` differs: here it is a MotherDuck
database `cdc_v2_t_d_<random>`, created for the test and dropped after it. Left out are the tests whose times are set for a
local database (deadlines of a fraction of a second) or that use a local mechanism; `test_md.py` has those again in
a form made for MotherDuck. What the tests of `test_from_apply.py` measure here goes to
`evidence/p3/destination/from_apply.json` (the fixture `measured`).

    tools/with-env uv run -q pytest -q -m md tests/destination/test_md_again.py
"""
import pytest

from test_alerts import (al,  # noqa: F401 - a fixture
                         test_an_alert_before_the_migrations_ran_fails_and_writes_nothing,
                         test_an_alert_is_gone_when_its_transaction_rolls_back,
                         test_emit_now_passes_on_what_it_cannot_decide,
                         test_emit_now_tries_again_after_a_lost_connection_and_records_once,
                         test_emit_now_uses_a_transaction_of_its_own,
                         test_emit_writes_one_row_inside_the_callers_transaction,
                         test_nothing_is_written_when_an_alert_is_refused, test_resolve,
                         test_the_table_carries_what_delivery_needs, test_two_alerts_in_one_transaction_and_defaults)
from test_fix_round import (test_a_changed_or_renamed_applied_step_refuses_to_start,
                            test_a_condition_on_a_column_the_transition_does_not_change_is_refused,
                            test_an_old_runs_plain_connection_cannot_write_after_a_take_over,
                            test_the_heartbeat_and_the_release_are_statements_of_the_layer,
                            test_a_connection_that_is_not_a_lane_registers_under_control_and_retries_a_conflict,
                            test_a_missing_file_is_refused_and_not_a_lost_connection,
                            test_a_registration_frozen_between_its_check_and_its_commit_and_a_take_over_in_between,
                            test_a_registration_while_a_take_over_is_open_does_not_commit,
                            test_a_returning_column_named_count_that_holds_text_is_no_count,
                            test_a_rolled_back_transaction_leaves_no_transition_row,
                            test_a_run_that_bypasses_the_fence_shows_a_duplicate_seq,
                            test_a_transition_row_carries_its_lane_epoch_seq_and_detail,
                            test_ddl_on_one_lane_while_another_lane_writes_is_a_conflict_and_runs_again,
                            test_seq_is_strictly_increasing_per_lane_with_gaps_only_at_take_overs_and_registrations,
                            test_the_misleading_statements_through_the_layer_are_refused,
                            test_the_state_module_and_alerts_write_only_inside_a_lane_transaction,
                            test_the_take_over_and_the_registration_never_put_the_counter_back,
                            test_three_simultaneous_starters_have_exactly_one_leader,
                            test_values_and_expect_in_the_same_conditional_update)
from test_from_apply import (test_a_registered_name_is_checked,
                             test_a_registered_name_is_gone_after_a_deadline_gave_the_connection_up,
                             test_a_registered_name_is_gone_when_the_block_ends_however_it_ends,
                             test_a_scratch_schema_of_apply_lives_inside_one_transaction,
                             test_a_value_in_a_destination_error_never_travels_on,
                             test_arrow_data_keeps_uuid_json_and_time_zones,
                             test_drop_schema_cascade_of_tables_made_in_the_same_transaction_fails_at_commit,
                             test_every_connection_of_the_layer_has_the_two_settings,
                             test_execute_one_never_takes_a_returned_value_for_a_count,
                             test_execute_returns_a_count_for_insert_update_and_delete_only,
                             test_fetch_arrow_gives_a_table_that_the_same_transaction_can_register,
                             test_merge_reports_no_count_and_truncate_counts_as_a_delete,
                             test_register_takes_a_table_a_record_batch_or_a_list_of_batches,
                             test_the_schema_check_and_the_migrations_leave_a_scratch_schema_alone,
                             test_the_two_settings_belong_to_the_connections_of_the_layer_only)
from test_lane import (test_a_failed_transaction_cannot_end_quietly,
                       test_a_lost_connection_is_reported_by_where_it_happened,
                       test_abort_from_another_thread_ends_the_call_at_once,
                       test_an_exception_rolls_back_and_is_passed_on,
                       test_an_unclassified_error_after_commit_was_sent_is_commit_unknown,
                       test_arrow_data_and_row_counts,
                       test_commit_runs_the_fence_statement_and_registers_the_connection,
                       test_conflicts_are_per_column_which_is_why_the_fence_statement_writes_the_epoch,
                       test_empty_transaction_still_runs_the_fence, test_every_lane_has_its_three_crash_points,
                       test_execute_one_needs_exactly_one_row, test_interrupt_earlier_and_cleanup,
                       test_leader_rows_cannot_be_deleted_or_inserted, test_misuse_is_refused,
                       test_new_connection_of_a_stale_epoch_is_refused_at_registration,
                       test_same_lane_same_epoch_twice_never_commits_both,
                       test_stale_epoch_gets_fence_lost_and_changes_nothing,
                       test_take_over_between_begin_and_the_fence_statement)
from test_schema import (lists,  # noqa: F401 - a fixture
                         test_a_database_that_does_not_fit_the_code_is_refused,
                         test_a_failing_step_leaves_nothing_half_done,
                         test_a_fresh_database_gets_the_tables_of_this_package,
                         test_a_missing_row_is_reported_not_repaired,
                         test_a_run_that_no_longer_leads_migrates_nothing,
                         test_a_step_whose_commit_has_no_answer_is_applied_once,
                         test_initialisation_creates_every_row_with_the_frozen_shape,
                         test_initialising_again_changes_nothing,
                         test_leaders_at_once_apply_every_step_exactly_once, test_only_the_control_lane_migrates,
                         test_running_again_does_nothing_and_writes_nothing,
                         test_steps_run_in_order_each_in_its_own_transaction)
from test_state import (ex,  # noqa: F401 - a fixture
                        test_a_transition_is_part_of_the_callers_transaction,
                        test_create_fails_when_the_key_exists_or_the_state_is_not_initial,
                        test_create_transition_and_read, test_every_pair_of_the_alert_machine,
                        test_every_pair_of_the_example_machines, test_keys,
                        test_the_wrong_stored_state_is_refused_and_the_transaction_stays_usable,
                        test_a_child_row_created_from_two_lanes_is_created_with_a_transition_of_its_parent,
                        test_a_key_is_created_only_by_the_lanes_that_the_machine_names,
                        test_two_rows_for_one_key_fail_the_transaction,
                        test_two_transactions_cannot_both_make_one_transition)

pytestmark = pytest.mark.md

__all__ = [name for name in dir() if name.startswith("test_")]


@pytest.fixture
def dest(md_dest):
    """The destination of the imported tests: a MotherDuck database of this test alone (see `conftest.md_dest`)."""
    return md_dest


@pytest.fixture
def measured(evidence):
    """``measured(key, value)`` of `test_from_apply.py`: here the value is written down as evidence."""
    def write(key, value):
        evidence("from_apply.json", key, value,
                 what="The seven points from the review of apply (contract C4), measured against MotherDuck")
    return write
