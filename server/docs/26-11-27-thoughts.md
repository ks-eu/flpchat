# LightChat

This for now will hold my plans, thoughts, etc. I guess this makes this a project notebook of
sorts?

## Personal Notes

### I am a beginner :)
This is my first major rust project, and my first major software engineering project, so a lot
of this is stuff I'm still learning. My previous experience is all scientific/mathematical
computing, pen and paper formal algorithms stuff. This also my first time really exploring
concurrent computing, so I am figuring things out as I go.

Obviously, as a Rust beginner (in the sense of me only really using it for around a month,
and being fully invested in this project for 2 weeks - 27th Nov 2026), I have a lot to learn
with regards to what good Rust code looks like. From the little experience I have, rust
is a "very" general programming language, so there is obviously lots of different ways one can
approach problems. I've honestly not had any problems with the borrow checker thus far, mostly
because the linter/lsp/ruff whatever its called, points it out before even having to compile, but
also i have some experience with C + the TCS adjacent background.

Concurrency is fun, I like trying to solve these problems, figuring out solutions to potential
race conditions. The custom communication protocol is a bit of a gimmick in all honesty, but
it probably saves time on wrangling with websockets/http and setting up APIs within that.
The added restriction of having the server run on old hardware is useful as well (i have some
old laptops that I am waiting on replacement chargers for, to set them up as headless servers).

### AI and stuff
With how prevalent AI is, and Rust's relative youngness, it is quite hard to find good human
primary sources on what the code should look like. The problem with languages with
a lot of features is that the docs are quite hard to follow. I am reinventing the wheel enjoyer,
so I do like to understand what the code is doing from "first principles". With AI writing
"all the code" now, and the internet basically becoming a barren wasteland for this kind of thing,
it does feel like, with Rust atleast, that the only real way to to figure it out is by using the
AI. The aim of this project is a polished piece of software that people will actually use.

I have also been using AI (codex) to work on my rust projects. It is mostly boilerplate writing
and tests (and using tdd principles to keep the agents on track). My workflow has two cases:

---

#### Case 1:
Feature I want to implement, no reference point.

I write the code after some pen and paper work/googling/asking AI.
Ask the AI to write tests for the code

---

#### Case 2:
Feature/Change I want to implement, has a reference point.

Write stubs (structs, enums, functions). give the agent inputs and expected outputs,
get it to write tests. then give it the reference point, plus some extra pointers,
get it to write the code using tests to make sure it works.

---

I've also used it to write the queries. I'm sure that when I get around to studying the
algebra/formal language behind queries, that I'd be able to write "good" queries, but I
want to focus on the rust. I know enough SQL to somewhat understand what the queries do,
(again this is from the mathematical, data sciency view point). Postgres databases, and the
use case here is different, requires a different type of query.

Coming back to the whole learning standpoint, it still doesn't fully feel comfortable to be
using AI like this. I will always not have full trust in outputs, atleast for the stage I'm at
with Rust. If you don't know what you don't know, then its hard to evaluate output. Treating
AI as the "expert" is not something that will ever sit right with me (whole other discussion).

im fine with agents writing tests, no one likes writing tests. i dont even read the test code most
the time, its just to guide the agents. The only tests that I really look at are the ones that
test things that I cant immediately check from following the logic of the code (race conditons
mostly). Agents also wrote the test running scripts, and the docker image for the test environment.
Things that I would like to figure how to do myself, but again, i want to focus on Rust. I know
what docker is, i know how its used etc. atleast for me, here its a case of "if it works it works".
SQL Queries are small enough here that I can read, and if i dont figure it out, then i can do
some quick research/ask the ai to do so.

### Writing good code
coming back to this point, writing good code. I want the code to follow good standards, but still
be "personal" to me in some sense. I definitely have certain preferences when it comes to writing
code. I do have to be aware of the fact that I am writing from scratch, and not working on an
existing code base, but it is still important. I have always seemed to be wary of OOP (whatever
OOP even means). Even in rust, methods defined over structs is also another thing i am a bit
uncomfortable with. I guess, since the application is "mine" and this isnt a "framework", its
fine for me to write this how i want, but I do want to get a better sense of how to write code
that other people can use easily. FLPC is part of my attempt to get better at this, with the
stated aim of publishing the rust implementation on crates.io (i'll have to do a big review of the
documentation though).

Rust has really nice features. I am aware that this definitely not a universal opinion, but
I really like Rust error handling (and the option enum as well). it is very useful, as it covers
every "possible" outcome in some sense. perhaps i need to write more code in languages outside of
python. its useful to have something tell you where the errors could happen precisely.

pattern matching is also very cool, i havent really got the hang of it yet, and it does feel like
one of those things that has a lot of different ways to do it. figuring out which way is best is
hard. (i should probably add a rust code review skill).

its hard, i was first introduced to rust properly from theprimeagen videos on it. i saw the stuff
about the learning curve and how it goes. the thing is, it does feel like, unless you are building
libraries for other people to use, then things like lifetimes and traits are things you can
avoid (especially traits). yes, sometimes you need to implement defined traits for types etc,
but ive seen advice that people should avoid writing custom traits unless the situation really
calls for it.

lifetimes also, everytime ive run into a "lifetime" issue, i get around it by just using a
variant type that satisfies that issue. I guess its experience and practice that will get me to
a point where I can comfortable know when to use these features well.

### Design
I feel like im starting to get a feel for what good design here is, from a purely code view point
(talk about architecture later) - this is general software engineering principles, not rust
specifically. I've started to think about how to really make the code modular. At this moment,
I havent actually written an end to end server for this flpc implementation. Everything is
components in modules. I am using an "actor model", where components communicate. there are
parts where shared memory might be useful, but it is so much easier to deal with than mutexes.

part of the process is "unspaghettifying" existing code. I think one of my problems with the
structs with methods thing (that are just an initialiser or methods that return fields) is that
it does tangle code up a bit. the first few structs i wrote for this project started off with a lot
of methods, but the moment i wanted to write things that spawned tasks etc, it just made more sense
to have stand alone functions that can be called wherever.

Part of why i havent gone end to end yet is that there are scheduling systems, and thread pools
and secondary run times that i want to think about and introduce. The places where this really
matters are:

- handling incoming connections: the verification process with argon2 is slow, even when run
  directly. since it is cpu intensive, there needs to be care with how many connections are handled
  at a time. I want it to be somewhat asynchronous, but obviously this needs to be adjustable
  based on the machine. I have 12 cores on my main computer, so i could probably dedicate a few
  cores to handling hashes (it would be cool to have a runtime/scheduler that sets out cores that
  when hash requests come in, are immediately blocked off and used for those tasks, but when the
  there arent any requests, allow them to be used for other tasks - this might be a thing already).
- sync: i wont be implementing this for a while, because syncing is intensive. It adds a lot of
  information to a queue, and could slow down message sending - as messages are by far the largest
  unit type.
- user requests: the main handler for the user table in the database will likely need a few
  runtimes. its on this "side" that the hashes are called, and the most expensive database
  writes are done. there are some calls that are cheaper than others (changeusername vs anything
  that involves hashing)

i want to be able to test a few runtimes/schedulers, so having the code be modular is important
here. it also allows for testing individual components. i will see the most benefit when writing
these routing functions. I dont need to have the components that handle end points written yet,
just need the channel types written.

I will probably need to do a big refactor of the code at somepoint. right now, the plan is to just
write code, and get to a working "solution" as soon as possible. once this is done, I will see
where abstractions can help, moving files and functions, structs enums around, check for
anti-patterns etc. It will requrie a lot, and probably more than the token allowance i have, but
it would still be useful. I would also like to possibly create a second implementation of flpc that
is a websocket API. I would need to learn how websockets work, but it would solve the problem of
having to pay to use cloudflare tunnels at this current moment. need to learn more about websockets,
but if a byte encoded API is a lot cheaper than just passing around strings, then this is worth
doing.
