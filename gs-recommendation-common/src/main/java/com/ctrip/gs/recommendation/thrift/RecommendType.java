package com.ctrip.gs.recommendation.thrift;

public enum RecommendType {
  SIGHT(0),
  TICKET(1),
  HOTEL(2);

  private final int value;

  private RecommendType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static RecommendType findByValue(int value) {
    switch(value) {
      case 0: return SIGHT;
      case 1: return TICKET;
      case 2: return HOTEL;
      default: return null;
    }
  }
}