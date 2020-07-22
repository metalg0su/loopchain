Feature: Consensus

  Background:
    Given DB does not exist
    And The network is running
    And I have enough balance

  Scenario: Send Tx
    Given I create tx
    When I send tx
    Then Tx Receipt should be valid
    And I can retrieve tx info

  Scenario: Leader Rotation
    Given I wait until block 14 is available

    When I get block [1-12]
    Then block [1-10] peer_id should be same
    And block [10-11] peer_id should be different
    And block [11-14] peer_id should be same

  Scenario: Generate block interval 2 sec
    # Block interval could be greater than 2 sec...
    Given I wait until block 4 is available

    When I get block [4-4]
    And I store timestamp
    And I wait until block 5 is available
    And I get block [5-5]
    Then Time diff less than 2.1 sec

  Scenario: Leader Complained
    Given I wait until block 4 is available
    And I kill Node0

    When I wait until block 7 is available
    And I get block [4-7]
    Then block [4-5] peer_id should be different
    And block [5-7] peer_id should be same
