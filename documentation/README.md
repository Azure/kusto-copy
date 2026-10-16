# 📚 Kusto Copy documentation

**Your next copy starts here.** Move data between Azure Data Explorer and Fabric Eventhouse
using your own ADLS Gen2 staging storage.

## Start here

1. **Get ready.** Follow the [setup guide](setup.md) to install the CLI, grant permissions,
   and prepare the destination table.
2. **Start copying.** Run a single-table copy:

   ```bash
   kc -s https://source.kusto.windows.net/sourcedb/mytable \
      -d https://dest.kusto.windows.net/destdb \
      -t https://storage.blob.core.windows.net/container/my-copy
   ```

   This copies historical data into the existing destination table with the same name.
   It does not clear that table first.
3. **Watch and verify.** [Read progress](progress.md), then
   [validate the copy](setup.md#validating-the-copy).

> 💡 **Keep your place:** use a dedicated staging folder for each independent copy.
> Reuse that folder when resuming the same job.

*Shell tip:* examples with `\` line continuations use Bash syntax. In PowerShell, put the
command on one line or use backticks for continuation.

## Guides and reference

| Guide | Use it to |
|-------|-----------|
| 🚀 [Setup](setup.md) | Install, assign permissions, and run your first copy |
| 🎯 [Common scenarios](scenarios.md) | Copy multiple tables, filter rows, or copy new arrivals |
| 🛠️ [Troubleshooting & performance](troubleshooting.md) | Fix errors, stalls, and slow copies |
| 🔬 [Advanced topics](advanced.md) | Explore the pipeline, security, resume, and operations |
| 📋 [CLI parameters](parameters.md) | Look up command-line options and defaults |
| ⚙️ [YAML configuration](yaml.md) | Define multiple copy activities |
| 📊 [Progress & monitoring](progress.md) | Interpret iteration states and block counts |

For available options in your installed version, run `kc --help`.
