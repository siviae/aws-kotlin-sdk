## Kotlin wrappers for AWS Java SDK clients
This library provides wrappers for async AWS SDK clients, which were designed 
for convenient usage in coroutine-based asynchronous code.

## Design principles:
### Simplicity over backwards compatibility
AWS SDK Java clients are designed for backwards-compatibility: they provide a lot of more or less useless classes even if particular method does not
 return 
meaningful response (like most of __Delete*__ methods), they return a particular response, which needs to be thought of.
Conversely, this library's wrapper classes return __Unit__ type in such case.
### Unified interface

### Light-weight implementation
### Kotlin-first

## Usage
## Status
Current library version: 0.0.2-SNAPSHOT


