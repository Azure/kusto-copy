# 📚 Kusto Copy Documentation

Welcome to the complete guide for **Kusto Copy** - your ultimate companion for seamless data migration between Fabric Eventhouse and Azure Data Explorer clusters!

## 🚀 Quick Navigation

<table>
<tr>
<td width="50%">

### 🏃‍♂️ **I'm New Here**
Just getting started? These guides will get you up and running:

- 🛠️ **[Setup Guide](setup.md)** - Install and configure Kusto Copy
- ⚡ **[First Copy](setup.md#your-first-copy)** - Run your first data migration
- 🎯 **[Common Scenarios](#common-scenarios)** - Popular use cases

</td>
<td width="50%">

### 🔧 **I Need Details** 
Ready to dive deep? Find the technical details here:

- 📋 **[CLI Parameters](parameters.md)** - Complete command reference
- 📄 **[YAML Configuration](yaml.md)** - Multi-table orchestration
- 📊 **[Progress Monitoring](progress.md)** - Track your migrations

</td>
</tr>
</table>

---

## 🎯 Common Scenarios

### 📦 Single Table Copy
**Need to move one table quickly?**
```bash
kc -s https://source.kusto.windows.net/sourcedb/mytable \
   -d https://dest.kusto.windows.net/destdb/ \
   -t https://storage.blob.core.windows.net/container/temp
```
👉 **[Full setup walkthrough](setup.md)**

### 🏢 Multi-Table Migration  
**Moving entire databases or multiple tables?**
Use YAML configuration for complex scenarios.

👉 **[YAML Configuration Guide](yaml.md)**

### 🔍 Filtered Data Copy
**Need to transform data during migration?**
Copy query results instead of full tables using the `-q` parameter.

👉 **[Query Parameter Guide](parameters.md#-q-query)**

---

## 🆘 Troubleshooting & Performance

### 🐌 **My copy is slow**
- Check [performance bottlenecks](progress.md#bottlenecks)
- Verify cluster capacity and scaling
- Review staging storage configuration

### ❌ **I'm getting errors**  
- Validate [prerequisites](setup.md#prerequisites)
- Check authentication and permissions
- Review [common error patterns](progress.md#troubleshooting)

### 📊 **I can't see progress**
- Learn to [read progress indicators](progress.md)
- Set up monitoring and logging
- Understand the copy pipeline stages

---

## 🔧 Advanced Topics

<details>
<summary><strong>🏗️ Architecture Deep Dive</strong></summary>

Understanding the three-stage pipeline:
1. **Export** - Data extraction from source
2. **Staging** - Temporary storage in Azure Storage  
3. **Ingestion** - Import into destination

</details>

<details>
<summary><strong>🔐 Authentication & Security</strong></summary>

- Service Principal authentication
- Managed Identity support
- Storage account permissions
- Network security considerations

</details>

<details>
<summary><strong>⚙️ Production Best Practices</strong></summary>

- Capacity planning guidelines
- Monitoring and alerting setup  
- Backup and recovery strategies
- Performance optimization tips

</details>

---

## 📖 Complete Reference

| Document | What's Inside |
|----------|---------------|
| **[Setup Guide](setup.md)** | Step-by-step installation and first copy |
| **[CLI Parameters](parameters.md)** | Complete command-line reference |
| **[YAML Schema](yaml.md)** | Configuration files for complex scenarios |
| **[Progress & Monitoring](progress.md)** | Understanding output and troubleshooting |

---

## 💡 Pro Tips

- 🎯 **Start small** - Test with a subset before copying large datasets
- 🔄 **Use staging strategically** - Choose storage regions close to your clusters  
- 📊 **Monitor progress** - Large copies can take hours, keep an eye on bottlenecks
- 🛡️ **Plan for interruptions** - Copies are resumable, don't worry about network hiccups

---

<div align="center">

**Need something that's not covered here?**  
Check out specific guides above or refer to command-line help with `kc --help`

</div> 