# Configuration Note

## ⚠️ Important: Do Not Modify `config.toml` in This Directory

The `config.toml` file in this directory is **automatically copied** from the project root during deployment by `deploy.sh`.

### Source of Truth

- **Edit:** `/config.toml` (project root)
- **Do NOT edit:** `/app/config.toml` (this directory)

### How It Works

1. You edit `config.toml` at the project root
2. Run `./deploy.sh`
3. The script copies `config.toml` → `app/config.toml`
4. The app is deployed with the copied config
5. The Databricks App reads `config.toml` from its working directory

### Why This Approach?

- Ensures the app can reliably access config in its working directory
- Keeps deployment simple and predictable
- Maintains single source of truth at project root
- `app/config.toml` is gitignored to prevent accidental commits

### To Update Configuration

```bash
# 1. Edit the root config file
vim config.toml  # or nano, code, etc.

# 2. Redeploy
./deploy.sh
```

The deployment script will automatically copy your changes to the app folder.
