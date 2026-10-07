---
name: new-operator-reference
description: After operator evaluation confirms a capability gap, write an implementation-ready build reference for exactly one operator.
---

# New Operator Build Reference

Write this reference only when the current evaluation proves that existing
operators cannot satisfy a required pipeline step. Describe exactly one operator.

```text
# Operator Build Reference, Round x
## Required New Operator
### Operator Name
- Meta operator type: ...
#### Purpose and Capabilities
...
#### Input Schema
...
#### Output Schema
...
#### Row and Field Preservation
...
#### Upstream and Downstream Operators
...
#### Additional Parameters
...
#### Input and Output Examples
...
#### Network Access Required
...
#### Acceptance Tests
...
```

Base the interface on retrieved operator documentation. Include enough behavioral
and testing detail for the Operator Builder Agent to implement it without guessing.
