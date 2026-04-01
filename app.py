"""ncluster — entry point."""

import threading

from flask import Flask

from server.config import APP_PORT
from server.db import init_db, cleanup_local_on_startup
from server.logbooks import migrate_legacy_files
from server.ssh import ssh_pool_gc_loop
from server.backup import backup_loop
from server.mounts import mount_health_loop
from server.routes import api

app = Flask(__name__)
app.register_blueprint(api)

if __name__ == "__main__":
    init_db()
    migrate_legacy_files()
    cleanup_local_on_startup()
    threading.Thread(target=ssh_pool_gc_loop, daemon=True).start()
    threading.Thread(target=backup_loop, daemon=True).start()
    threading.Thread(target=mount_health_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=APP_PORT, debug=False)
