//! Module containing artifacts that can be resolved and installed.
mod sdist;

mod extract;
mod stree;
/// Module for working with PyPA wheels. Contains the [`ArchivedWheel`] type, and related functionality.
pub mod wheel;

pub use sdist::SDist;
pub use stree::STree;
pub use wheel::ArchivedWheel;
