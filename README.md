## Kotlin wrappers for AWS Java SDK clients
This library provides Kotlin wrappers for async AWS SDK clients, which were designed 
for convenient usage in coroutine-based asynchronous code.

## Design principles:
### Simplicity over backwards compatibility
Unlike official AWS SDK Java clients, which API is very extensible in a backwards-compatible way, this library tries 
to keep the API minimalistic. It means that breaking changes could occur if new AWS SDK update will require so, but they should be made only if it is
necessary as long as stable version of the library (1.0.0) released.
### Unified interface
Since AWS Java SDK API matches AWS REST API, and APIs for different services made by different teams, there are a lot of places in SDK which should
work similarly and have the same interface, but they don't. This library tries to unify these API differences.
### Light-weight implementation
To be suitable for usage in serverless environment, this library uses only provided dependencies, no dependencies besides AWS SDK and Kotlin and
there are separate artifacts for each AWS service.
### Kotlin-first
The library built for usage from Kotlin code and there are no intention to make it convenient for Java users.

## Usage
TBD 

## Status
Current library version: 0.0.2.RELEASE


