# Server Parameters
Mongo database and router servers (i.e., `mongod` and `mongos`) provide a number of configuration 
options through server parameters. These parameters allow users to configure the behavior of the 
server at startup or runtime. For instance, `logLevel` is a server parameter that configures the 
logging verbosity.

## How to define new parameters
Parameters are defined by the elements of the `server_parameters` section of an IDL file. The IDL 
machinery will parse these files and generate C++ code, and corresponding header files where 
appropriate. The generated code will self-register server parameters with the runtime.

Consider `logLevel` parameter from [`parameters.idl`][parameters.idl] for example:
```yaml
...
server_parameters:
  ...
  logLevel:
    description: "Specifies the verbosity of logging"
    set_at: ["startup", "runtime"]
    cpp_class:
      name: LogLevelServerParameter
      override_set: true
  ...
```

This defines a server parameter called `logLevel`, which is settable at startup or at runtime, and 
declares a C++ class for the parameter (i.e., `LogLevelServerParameter`). Refer to the 
[Server Parameters Syntax](#server-parameters-syntax) documentation for the complete IDL syntax.

## How to change a defined parameter
Users can set or modify a server parameter at startup and/or runtime, depending on the value 
specified for `set_at`. For instance, `logLevel` may be set at both startup and runtime, as 
indicated by `set_at` (see the above code snippet).

At startup, server parameters may be set using the `--setParameter` command line option. 
At runtime, the `setParameter` command may be used to modify server parameters. 
See the [`setParameter` documentation][set-parameter] for details.

## How to get the value provided for a parameter
Server developers may retrieve the value of a server parameter by:
* Accessing the C++ expression that corresponds to the parameter of interest. For example, reading 
from [`serverGlobalParams.quiet`][quiet-param] returns the current value for `quiet`.
* Registering a callback to be notified about changes to the server parameter (e.g., 
[`onUpdateFTDCFileSize`][ftdc-file-size-param] for `diagnosticDataCollectionFileSizeMB`).

Database users may use the [`getParameter`][get-parameter] command to query the current value for a 
server parameter.

## Server Parameters Syntax
The following shows the IDL syntax for declaring server parameters. Field types are denoted in each 
section. For details regarding `string or expression map`, see that section 
[below](#string-or-expression-map).

```yaml
server_parameters:
  "nameOfParameter": # string
    set_at: # string or list of strings
    description: # string
    cpp_vartype: # string
    cpp_varname: # string
    cpp_class: # string (name field) or map
      name: # string
      data: # string
      override_ctor: # bool
      override_set: # bool
    redact: # bool
    test_only: # bool
    default: # string or expression map
    deprecated_name: # string or list of strings
    on_update: # string
    condition:
      expr: # C++ bool expression, runtime evaled
      constexpr: # C++ bool expression, compile-time eval
      preprocessor: # C preprocessor condition
    validator: # Map containing one or more of the below
      lt: # string or expression map
      gt: # string or expression map
      lte: # string or expression map
      gte: # string or expression map
      callback: # string
```
Each entry in the `server_parameters` map represents one server parameter. The name of the parameter 
must be unique across the server instance. More information on the specific fields:

* `set_at` (required): Must contain one or both of the values `startup` and/or `runtime`. If runtime 
is specified along with `cpp_varname`, then `decltype(cpp_varname)` must refer to a thread-safe 
storage type, specifically: `AtomicWord<T>`, `AtomicDouble`, `std::atomic<T>`, or 
`boost::synchronized<T>`. 

* `description` (required): Free-form text field currently used only for commenting the generated C++ 
code. Future uses may preserve this value for a possible `{listSetParameters:1}` command or other 
programmatic and potentially user-facing purposes.

* `cpp_vartype`: Declares the full storage type. If `cpp_vartype` is not defined, it may be inferred 
from the C++ variable referenced by `cpp_varname`.

* `cpp_varname`: Declares the underlying variable or struct member to use when setting or reading the 
server parameter. If defined together with `cpp_vartype`, the storage will be declared as a global 
variable, and externed in the generated header file. If defined alone, a variable of this name will 
assume to have been declared and defined by the implementer, and its type will be automatically 
inferred at compile time. If `cpp_varname` is not defined, then `cpp_class` must be specified.

* `cpp_class`: Declares a custom `ServerParameter` class in the generated header using the provided 
string, or the name field in the associated map. The declared class will require an implementation 
of `setFromString()`, and optionally `set()`, `append()`, and a constructor.
See [Specialized Server Parameters](#specialized-server-parameters) below.

* `default`: String or expression map representation of the initial value.

* `redact`: Set to `true` to replace values of this setting with placeholders (e.g., for passwords).

* `test_only`: Set to `true` to disable this set parameter if `enableTestCommands` is not specified.

* `deprecated_name`: One or more names which can be used with the specified setting and underlying 
storage. Reading or writing a setting using this name will result in a warning in the server log.

* `on_update`: C++ callback invoked after all validation rules have completed successfully and the 
new value has been stored. Prototype: `Status(const cpp_vartype&);`

* `condition`: Up to three conditional rules for deciding whether or not to apply this server 
parameter. `preprocessor` will be evaluated first, followed by `constexpr`, then finally `expr`. If 
no provided setting evaluates to `false`, the server parameter will be registered.

* `validator`: Zero or many validation rules to impose on the setting. All specified rules must pass 
to consider the new setting valid. `lt`, `gt`, `lte`, `gte` fields provide for simple numeric limits 
or expression maps which evaluate to numeric values. For all other validation cases, specify 
callback as a C++ function or static method. Note that validation rules (including callback) may run 
in any order. To perform an action after all validation rules have completed, `on_update` should be 
preferred instead. Callback prototype: `Status(const cpp_vartype&);`

Any symbols such as global variables or callbacks used by a server parameter must be imported using 
the usual IDL machinery via `globals.cpp_includes`. Similarly, all generated code will be nested 
inside the namespace defined by `globals.cpp_namespace`. Consider the following for example:
```yaml
global:
  cpp_namespace: "mongo"
  cpp_includes:
    - "mongo/util/net/ssl_parameters.h"

server_parameters:
  opensslCipherConfig:
    ...
    validator:
      callback: "validateOpensslCipherConfig" # The callback is declared in "ssl_parameters.h"
```

### Specialized Server Parameters
When `cpp_class` is specified on a server parameter, a child class of `ServerParameter` will be 
created in the `gen.h` file named for either the string value of `cpp_class`, or if it is expressed 
as a dictionary, then `cpp_class.name`. A `cpp_class` directive may also contain:
```yaml
server_parameters:
  someParameter:
    cpp_class:
      name: string # Name to assign to the class (e.g., SomeParameterImpl)
      data: string # cpp data type to add to the class as a property named "_data"
      override_ctor: bool # True to allow defining a custom constructor, default: false
      override_set: bool # True to allow defining a custom set() method, default: false
```

`override_ctor`: If `false`, the inherited constructor from the `ServerParameter` base class will be 
used. If `true`, then the implementer must provide a 
`{name}::{name}(StringData serverParameterName, ServerParameterType type)` constructor. In addition 
to any other work, this custom constructor must invoke its parent's constructor.

`override_set`: If `true`, the implementer must provide a `set` member function as:
```cpp
Status {name}::set(const BSONElement& val);
```
Otherwise the base class implementation `ServerParameter::set` is used. It
invokes `setFromString` using a string representation of `val`, if the `val` is
holding one of the supported types.

If `param.redact` was specified as `true`, then a standard append method will be provided which 
injects a placeholder value. If `param.redact` was not specified as `true`, then an implementation 
must be provided with the following signature: 

```cpp
Status {name}::append(OperationContext*, BSONObjBuidler&, const std::string&);
```

Lastly, a `setFromString` method must always be provided with the following signature:
```cpp
Status {name}::setFromString(const std::string& value);
```

Each server parameter encountered will produce a block of code to run at process startup similar to 
the following:
```cpp
/**
 *  Iteration count to use when creating new users with
 *  SCRAM-SHA-1 credentials
 */
MONGO_COMPILER_VARIABLE_UNUSED auto* scp_unique_ident = [] {
    using T = decltype(saslGlobalParams.scramSHA1IterationCount);
    constexpr auto setAt = ServerParameterType::kStartupAndRuntime;
    auto ret = new IDLServerParameterWithStorage<T>(
            "scramIterationCount",
            saslGlobalParams.scramSHA1IterationCount, setAt);
    ret->addBound<predicate::GTE>(5000);
    return ret;
}();
```

Any additional validator and callback would be set on `ret` as determined by the server parameter 
configuration block.

### String or Expression Map
The default and implicit fields above, as well as the `gt`, `lt`, `gte`, and `lte` validators accept 
either a simple scalar string which is treated as a literal value, or a yaml map containing an 
attribute called `expr`, which must be a string containing an arbitrary C++ expression to be used 
as-is. Optionally, an expression map may also include the `is_constexpr: false` attribute, which 
will suspend enforcement of the value being a `constexpr`.

For example, consider:
```yaml
server_parameters:
  connPoolMaxInUseConnsPerHost:
    ...
    cpp_varname: maxInUseConnsPerHost
    default:
      expr: std::numeric_limits<int>::max()
    ...
```

Here, the server parameter's default value is the evaluation of the C++ expression 
`std::numeric_limits<int>::max()`. Additionally, since default was not explicitly given the 
`is_constexpr: false` attribute, it will be round-tripped through the following lambda to guarantee 
that it does not rely on runtime information.
```cpp
[]{ constexpr auto value = <expr>; return value; }()
```

[parameters.idl]: ../src/mongo/db/commands/parameters.idl
[set-parameter]: https://docs.mongodb.com/manual/reference/parameters/#synopsis
[get-parameter]: https://docs.mongodb.com/manual/reference/command/getParameter/#getparameter
[quiet-param]: https://github.com/mongodb/mongo/search?q=serverGlobalParams+quiet+extension:idl&type=code
[ftdc-file-size-param]: ../src/mongo/db/ftdc/ftdc_server.idl
