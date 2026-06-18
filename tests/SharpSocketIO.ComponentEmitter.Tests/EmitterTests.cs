using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using Xunit;

namespace SharpSocketIO.ComponentEmitter.Tests;

public class EmitterTests
{
    [Fact]
    public void On_adds_listeners_in_order()
    {
        var emitter = new Emitter();
        var calls2 = new List<object>();
        emitter.On("foo", args => { calls2.Add("one"); calls2.Add(args[0]); });
        emitter.On("foo", args => { calls2.Add("two"); calls2.Add(args[0]); });

        emitter.Emit("foo", 1);
        emitter.Emit("bar", 1);
        emitter.Emit("foo", 2);

        Assert.Equal(new object[] { "one", 1, "two", 1, "one", 2, "two", 2 }, calls2);
    }

    [Fact]
    public void On_supports_Object_prototype_method_names_as_events()
    {
        var emitter = new Emitter();
        var calls = new List<object>();
        emitter.On("constructor", args => { calls.Add("one"); calls.Add(args[0]); });
        emitter.On("__proto__", args => { calls.Add("two"); calls.Add(args[0]); });

        emitter.Emit("constructor", 1);
        emitter.Emit("__proto__", 2);

        Assert.Equal(new object[] { "one", 1, "two", 2 }, calls);
    }

    [Fact]
    public void Once_adds_a_single_shot_listener()
    {
        var emitter = new Emitter();
        var calls = new List<object>();
        emitter.Once("foo", args => { calls.Add("one"); calls.Add(args[0]); });

        emitter.Emit("foo", 1);
        emitter.Emit("foo", 2);
        emitter.Emit("foo", 3);
        emitter.Emit("bar", 1);

        Assert.Equal(new object[] { "one", 1 }, calls);
    }

    [Fact]
    public void Off_removes_a_specific_listener()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");

        emitter.On("foo", One);
        emitter.On("foo", Two);
        emitter.Off("foo", Two);
        emitter.Emit("foo");

        Assert.Equal(new[] { "one" }, calls);
    }

    [Fact]
    public void Off_works_with_once()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");

        emitter.Once("foo", One);
        emitter.Once("fee", One);
        emitter.Off("foo", One);
        emitter.Emit("foo");

        Assert.Empty(calls);
    }

    [Fact]
    public void Off_works_when_called_from_within_a_handler()
    {
        var emitter = new Emitter();
        bool called = false;
        void B() => called = true;
        emitter.On("tobi", _ => emitter.Off("tobi", B));
        emitter.On("tobi", B);
        emitter.Emit("tobi");
        Assert.True(called);
        called = false;
        emitter.Emit("tobi");
        Assert.False(called);
    }

    [Fact]
    public void Off_event_removes_all_listeners_for_that_event()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");

        emitter.On("foo", One);
        emitter.On("foo", Two);
        emitter.Off("foo");
        emitter.Emit("foo");
        emitter.Emit("foo");

        Assert.Empty(calls);
    }

    [Fact]
    public void Off_specific_removes_event_array_when_last_subscriber_leaves()
    {
        var emitter = new Emitter();
        void Cb1() { }
        void Cb2() { }
        emitter.On("foo", Cb1);
        emitter.On("foo", Cb2);
        emitter.Off("foo", Cb1);
        Assert.Single(emitter.Listeners("foo"));
    }

    [Fact]
    public void Off_no_args_removes_all_listeners()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");
        emitter.On("foo", One);
        emitter.On("bar", Two);
        emitter.Emit("foo");
        emitter.Emit("bar");
        emitter.Off();
        emitter.Emit("foo");
        emitter.Emit("bar");
        Assert.Equal(new[] { "one", "two" }, calls);
    }

    [Fact]
    public void Listeners_returns_callbacks_when_present()
    {
        var emitter = new Emitter();
        void Foo() { }
        emitter.On("foo", Foo);
        Assert.Single(emitter.Listeners("foo"));
    }

    [Fact]
    public void Listeners_returns_empty_when_absent()
    {
        var emitter = new Emitter();
        Assert.Empty(emitter.Listeners("foo"));
    }

    [Fact]
    public void HasListeners_true_when_present()
    {
        var emitter = new Emitter();
        emitter.On("foo", () => { });
        Assert.True(emitter.HasListeners("foo"));
    }

    [Fact]
    public void HasListeners_false_when_absent()
    {
        var emitter = new Emitter();
        Assert.False(emitter.HasListeners("foo"));
    }

    [Fact]
    public void EmitReserved_emits_like_emit()
    {
        var emitter = new Emitter();
        object? got = null;
        emitter.On("decoded", args => got = args[0]);
        emitter.EmitReserved("decoded", 42);
        Assert.Equal(42, got);
    }
}
