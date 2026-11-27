//! this submodule contains routing functions (i.e the functions that send
//! data to different running processes)
//! these are also functions that convert given units/input into their server response
//! variant.
//! 
//! its a sub module because there are quite a few scenarios to handle
//! 
//! note, broadcast here means sending to the mpsc channels for all users in the server
//!
//! message unit: they are received with the sender's id. they need to be assigned a LimId,
//!               then converted into a ServerMessage. this is routed to every joined user
//!               and the messages database handler.
//!
//! private message unit: same as message unit, but sent only to the target and messages database
//!                       handler, if user isnt online, the sender gets a UserNotOnline
//!
//! userjoin unit: this will be packaged with a writer_tx, the writer_tx and id is added
//!                to a hashmap, which is used for routing and the userjoin is broadcast to
//!                everyone
//!
//! userleft unit: this removes the id and writer_tx from hashmap, broadcast to remaining users
//!
//! userregistered unit: this is sent from the handle_register function when successful, broadcast
//!                      to all logged in users
//!
//! userremoved unit: this sent from either handle_removeself, or the future admin request handler.
//!                   broadcast to all logged in users
//!
//! admin messages: recieved with sender id. first, checked against the id: admin hashmap, if the
//!                 user has admin powers, this is routed to admin request handler. if not, the
//!                 user that requested is sent a NotAdmin unit (needs implementing)
//!
//! sync requests: routed to the sync handler
//!
//! changeusername request: recieved with sender id, sent to userhandler (requires a fail message
//!                         response)
//!
//! changedusername: send to all users
//!
//! ping: routed back to sender (also need a connectionhandler variant)
//!
//! in future, will need a changepassword unit (this is handled by the connection handler, as
//! it requires verification input)
//!
//! some units will be given a "broadcastable" trait, some units will be given a
//! "single receiver" trait. private messages are sent to both.
//!
//! desperately need a servermessagefail and serverprivatemessagefail variant.
//! could just send 0s? its not clean though.
