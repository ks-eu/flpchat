//! module handles onboarding, which is the process that sits between a client being verified to
//! log in and actually joining the server. This is particularly important for this protocol, as
//! users are identified by a unique 2 byte id.
//!
//! when someone joins for the first time, they need to receive a list of the registered users
//! on the server.
//!
//! every time they login, they need to be sent a list of all the users logged in to the server at
//! that time.
//!
//! we store the time that a user last left, so that we can compare it to the time they joined so
//! that they can get an up to date list of any changes during that time (user registration and
//! user removes). this means that we need to add a table that tracks these events.
//!
//! this means that client apps are able to correctly render received messages with the username
//! of the sender.
//!
