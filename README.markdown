# An erlang conc list implementation.

Read more [in my blog post][post].

[post]: http://dustin.github.com/2010/03/04/erlang-conc.html

Rebalancing is pretty slow right now, so initial list builds, appends,
etc... take longer than they should.  If you're reading this, I expect
a patch. :)

## Concurrent Awesomeness

Make a 1,000 node balanced conc list

    C = conc:from_list(lists:seq(1, 1000)), ok.

Now do something expensive with it (on my system, this takes about 4s):

    conc:foreach(fun(_X) -> timer:sleep(1000) end, C).
