# Helper Functions

Use `@helper` to turn a regular function into a `Helper` with trackable source code. This is actually its only difference from a regular function. A typical use case is custom metric.

{% hint style="danger" %}
In old versions of KTS, helpers autoreloaded before each call. It would allow you to inject code in runtime. For example, you could pass such a helper as a metric and during validation process change it to print debug output or something. 

In new KTS this usage is not expected and may lead to undefined behavior. Please don't do so until we officially return it as a feature.
{% endhint %}



