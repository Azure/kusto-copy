# Coding Standard

These rules apply to contributors and AI agents.

## File Structure

Each class should have its own file unless it is a private class within another class.

Files must end with their final visible content character. Do not add a terminal newline or a
blank line after the final content. The last visible line in a C# file must be the closing brace
of the final block.

## Formatting

All lines in Markdown or C# files must be fewer than 100 characters.

Wrap long method argument lists so that each argument occupies its own line:

```
var result = myObject.InvokingMethod(
    firstParameter,
    secondParameter,
    thirdParameter);
```

Always use curly braces for `if` statements, even when a clause has one statement.