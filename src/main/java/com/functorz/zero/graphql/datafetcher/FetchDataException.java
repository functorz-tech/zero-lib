package com.functorz.zero.graphql.datafetcher;

public class FetchDataException extends RuntimeException {
  public FetchDataException(Exception e) {
    super(e);
  }
}
