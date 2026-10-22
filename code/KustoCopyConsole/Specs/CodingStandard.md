# Coding Standard

These rules apply to contributors and AI agents.

## File Structure

Each class should have its own file unless it is a private class within another class.

Files must end with their final visible content character. This means:
- no terminal newline after the final byte
- no blank lines after the final visible content
- the final visible line in a C# file must be the closing brace of the final block
- do not leave trailing whitespace on the last line

This rule is mandatory and is not optional formatting. A file is considered invalid if it ends in
an empty line or any newline character after its last content character.

## Formatting

All lines in Markdown or C# files must be fewer than 100 characters.

This is a hard rule. Do not exceed 100 characters for a line; wrap the line or refactor the code
before finishing the change. A change that introduces a long line is not acceptable even if the file
otherwise compiles.

Wrap long method argument lists so that each argument occupies its own line:

```
var result = myObject.InvokingMethod(
    firstParameter,
    secondParameter,
    thirdParameter);
```

Always use curly braces for `if` statements, even when a clause has one statement.