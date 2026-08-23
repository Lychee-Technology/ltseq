# Rust Project Development Standards

How this project writes Rust: idiomatic, maintainable, and safe to refactor.

---

## 1. Project Structure

Use Cargo workspaces for multi-crate systems, with the standard folders: `src/`,
`tests/`, `bench/`, `examples/`. Keep the public API minimal and the
implementation details hidden behind it.

---

## 2. Idiomatic Rust Practices

Prefer borrowing over cloning. It costs less, and it usually makes ownership
clearer at the same time.

Handle errors with `Result` and `Option`, and give them custom error types
(`thiserror`, `anyhow`). Production code does not call `unwrap`.

CI enforces Clippy and rustfmt.

---

## 3. Code Quality and Code Smells in Rust

A code smell is a sign of a deeper design or structural problem: something that
slows development, hurts maintainability, or accumulates technical debt. Smells
usually do not stop the code from running correctly. They mark where refactoring
would pay off.

### 3.1 Excessive ownership transfer / excessive cloning

Frequent `.clone()`, `Arc`, `Rc`, or `String` on arguments that could be
borrowed. Every one is a heap allocation you did not need, and the cost stays
hidden because nothing looks wrong at the call site.

- Prefer a borrow (`&T`), a generic `AsRef<T>`, or `Cow` where possible.
- Refactor API signatures to accept references rather than owned types.
- Limit `Arc`/`Rc` to genuine shared-ownership contexts.

### 3.2 Lazy default initialization

`..Default::default()` without explicit field handling. It buries assumptions
about what those values are, and it turns every later field addition into a
candidate bug.

Initialize all fields explicitly, or destructure the default so every field is
visible at construction. Then the compiler catches the paths you missed.

### 3.3 Long functions / modules

Functions or modules carrying many lines or many responsibilities. They are hard
to read, hard to test, and hard to refactor. Extract smaller helpers and
decompose along single responsibilities.

### 3.4 Long parameter lists

Functions taking many parameters are hard to read, hard to use, and easy to get
wrong at the call site. Group the related ones into a struct, or use a builder.

### 3.5 Primitive obsession

`i32` and `String` standing in for domain concepts, at the cost of clarity and
type safety. Use newtype structs so the concept carries its meaning in the type.

### 3.6 Feature envy / over-accessing other structs

A method in one type that keeps reaching into another type's internals. The two
end up tightly coupled and the API turns brittle. Move the logic into the type
that owns the data, or put the shared behavior behind a trait.

### 3.7 Inappropriate intimacy between types

One struct depending closely on another's internal structure. Encapsulation is
gone, and a change in one ripples through the system. Provide proper accessor
methods, and use traits to loosen the coupling.

### 3.8 Middle man

A struct that only delegates to another without behavior of its own: redundant
abstraction, unnecessary indirection. Remove the intermediary and let callers
reach the real thing. If the abstraction is genuinely needed, give it a job.

### 3.9 Message chains

Long chains like `a.b().c().d()`. They break whenever anything in the middle
moves, and they hide where the abstraction boundaries are. Collapse the chain
behind a clear API.

Classic smell categories apply in Rust too, whenever they correlate with
maintenance burden: duplicated code, dead code, shotgun surgery, parallel
hierarchies, speculative generality, and data clumps.

---

## 4. Refactoring Strategies

Find smells through code review or through metrics such as function length and
complexity. Before changing anything, protect the current behavior with tests.
Then refactor in small steps, leaning on the compiler and the type system to
catch what you break, and weigh what each step costs in readability and API
churn.

---

## 5. Design Patterns (Rust Interpretation)

Rust's type system often makes classical OO patterns unnecessary, or shows them
to be a poor fit. Traits, enums, and composition cover much of what Abstract
Factory or Decorator address in other languages. The idiomatic patterns that do
earn their keep:

### 5.1 Creational

Builder for complex settings; trait-bound constructors for controlled
instantiation.

### 5.2 Structural

Newtype wrappers for type safety and behavior extension; Facade or Adapter over
low-level APIs.

### 5.3 Behavioral

Strategy via generics and traits. The iterator pattern with combinators. State
enums plus pattern matching, in place of polymorphic classes.

---

## 6. Testing and Verification

Unit tests live within their module (`#[cfg(test)]`), integration tests under
`tests/`. Use property testing (`proptest`) for invariants. Refactoring happens
behind a test safety net, so behavior survives the change.

---

## 7. Documentation and API Stability

Document public APIs with rustdoc, examples included. Version crate releases
semantically, and generate the docs through `cargo doc` automation.

---

## 9. Performance Considerations

Prefer zero-cost abstractions and avoid redundant allocations. Past that, profile
the hotspots and optimize only where the profile says it matters.

---

## 10. Continuous Maintenance Culture

Code smells are forward signals for improvement, not failures. They are
heuristics, and what they point at is where refactoring would buy clarity,
modularity, and maintainability.

Readability and maintainability are first-class metrics in reviews and in team
practice.
