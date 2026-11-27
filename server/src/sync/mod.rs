//! this module covers the sync handling. the nature of syncs, especially message syncs, mean that
//! they need to be handled carefully to prevent overloading writer channels. this means
//! implementing scheduling, and processes to limit database reads to a minimum (maintaining a small
//! cache of messages, currently registered users stored somewhere on memory), rate limiting syncs
//! etc.
//!
//! this again probably means adding more functionality to flpc.
//!
