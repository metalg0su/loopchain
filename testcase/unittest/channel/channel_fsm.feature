Feature: TestChannelStateMachine

Scenario: init channel state
  Given community node is initialized
  When trigger complete_init_components
  Then the state should be EvaluateNetwork

Scenario: init community node
  Given community node is initialized
  And trigger complete_init_components
  And trigger block_sync
  And trigger complete_sync
  When trigger complete_subscribe
  Then the state should be BlockGenerate

Scenario: init citizen node
  Given citizen node is initialized
  And trigger complete_init_components
  And trigger block_sync
  And trigger complete_sync
  When trigger complete_subscribe
  Then the state should be Watch

Scenario: trigger turn_to_peer twice and timer called only once
  Given community node is initialized
  And trigger complete_init_components
  And trigger block_sync
  And trigger complete_sync
  And trigger complete_subscribe
  When trigger turn_to_leader
  And trigger turn_to_leader
  Then timer_should_be_called_only_at_once
