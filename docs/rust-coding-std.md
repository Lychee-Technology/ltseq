# Rust Project Development Standards

How this project writes Rust: idiomatic, maintainable, and safe to refactor. The
material draws on established software engineering principles, idiomatic Rust
conventions, design pattern practice, and the code smell / refactoring
literature.

---

## 1. Project Structure

- Cargo workspaces for multi-crate systems.
- Standard folders: `src/`, `tests/`, `bench/`, `examples/`.
- Expose a minimal public API; hide implementation details.

---

## 2. Idiomatic Rust Practices

- Ownership and borrowing: prefer borrowing over cloning, for performance and
  for clarity.
- Error handling: use `Result`/`Option` and custom error types (`thiserror`,
  `anyhow`). No `unwrap` in production code.
- Clippy and rustfmt: enforce linting and formatting in CI.

---

## 3. Code Quality and Code Smells in Rust

A code smell is a sign of a deeper design or structural problem: something that
slows development, hurts maintainability, or accumulates technical debt. Smells
usually do not stop the code from running correctly. They mark where refactoring
would pay off.

The Rust-relevant smells below come with detection hints and a way out.

---

### 3.1 Excessive ownership transfer / excessive cloning

Smell: frequent `.clone()`, `Arc`, `Rc`, or `String` on arguments that could be
borrowed.

Impact: redundant heap allocations and hidden performance cost.

Fix:

- Prefer a borrow (`&T`), a generic `AsRef<T>`, or `Cow` where possible.
- Refactor API signatures to accept references rather than owned types.
- Limit `Arc`/`Rc` to genuine shared-ownership contexts.

---

### 3.2 Lazy default initialization

Smell: `..Default::default()` without explicit field handling.

Impact: implicit assumptions about values, and future bugs when fields are
added.

Fix: initialize all fields explicitly, or destructure the default so every field
is visible at construction. That way the compiler catches missing handling.

---

### 3.3 Long functions / modules

Smell: functions or modules with many lines or many responsibilities.

Impact: hard to read, test, and refactor.

Fix: extract smaller helper functions; decompose along single responsibilities.

---

### 3.4 Long parameter lists

Smell: functions taking many parameters.

Impact: hard to read and use, and easy to get wrong at the call site.

Fix: group related parameters into structs, or use a builder.

---

### 3.5 Primitive obsession

Smell: primitive types (`i32`, `String`) standing in for domain concepts.

Impact: less clarity and less type safety.

Fix: use newtype structs so the domain concept carries meaning.

---

### 3.6 Feature envy / over-accessing other structs

Smell: a method in one type keeps reaching into another type's internals.

Impact: tight coupling and brittle API usage.

Fix: move the logic into the type that owns the data, or use a trait to
encapsulate the shared behavior.

---

### 3.7 Inappropriate intimacy between types

Smell: one struct depends closely on another's internal structure.

Impact: broken encapsulation; changes ripple through the system.

Fix: provide proper accessor methods, and use traits to reduce coupling.

---

### 3.8 Middle man

Smell: a struct that only delegates to another without behavior of its own.

Impact: redundant abstraction and unnecessary indirection.

Fix: remove the intermediary and provide direct access. If the abstraction is
needed, give it a purpose.

---

### 3.9 Message chains

Smell: long chains like `a.b().c().d()`.

Impact: fragile under refactoring; hides abstraction boundaries.

Fix: collapse the chain behind a clear API.

---

Classic smell categories apply in Rust too, whenever they correlate with
maintenance burden: duplicated code, dead code, shotgun surgery, parallel
hierarchies, speculative generality, and data clumps.

---

## 4. Refactoring Strategies

- Identify smells through code review or metrics (function length, complexity).
- Protect behavior with tests before refactoring.
- Refactor in small steps, leaning on the compiler and the type system.
- Check the maintenance implications: readability, API changes.

---

## 5. Design Patterns (Rust Interpretation)

Rust's type system often makes classical OO patterns unnecessary, or shows them
to be a poor fit. Traits, enums, and composition cover much of what Abstract
Factory or Decorator address in other languages.

Idiomatic patterns worth reaching for:

### 5.1 Creational

- Builder, for complex settings.
- Trait-bound constructors, for controlled instantiation.

### 5.2 Structural

- Newtype wrappers, for type safety and behavior extension.
- Facade/Adapter over low-level APIs.

### 5.3 Behavioral

- Strategy via generics and traits.
- The iterator pattern with combinators.
- State enums plus pattern matching, instead of polymorphic classes.

---

## 6. Testing and Verification

- Unit tests within modules (`#[cfg(test)]`).
- Integration tests under `tests/`.
- Property testing (`proptest`) for invariants.
- Refactor behind a test safety net so behavior is preserved.

---

## 7. Documentation and API Stability

- Rustdoc for public APIs, with examples.
- Semantic versioning for crate releases.
- `cargo doc` automation for doc generation.

---

## 9. Performance Considerations

- Prefer zero-cost abstractions.
- Avoid redundant memory allocations.
- Profile hotspots and optimize only where it matters.

---

## 10. Continuous Maintenance Culture

- Treat code smells as forward signals for improvement, not as failures. They
  are heuristics that point at where to refactor for clarity, modularity, and
  maintainability.
- Make readability and maintainability first-class metrics in reviews and team
  practice.
