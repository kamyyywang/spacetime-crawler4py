# STARTER CACHE REGISTRATION, WITH SAVED-STATE RECOGNITION
# Registration returns the assigned download cache endpoint. It is separate
# from fetching a page and is the only reason launch needs the spacetime Node.
# A normal resume recognizes both the SQLite frontier and older shelf formats.
import os
from spacetime import Node
from utils.pcc_models import Register

def init(df, user_agent, fresh):
    # The course service identifies this crawl by the configured group ID string.
    # Push registration, wait for an assignment, then remove this temporary record.
    reg = df.read_one(Register, user_agent)
    if not reg:
        reg = Register(user_agent, fresh)
        df.add_one(Register, reg)
        df.commit()
        df.push_await()
    while not reg.load_balancer:
        df.pull_await()
        if reg.invalid:
            raise RuntimeError("User agent string is not acceptable.")
        if reg.load_balancer:
            df.delete_one(Register, reg)
            df.commit()
            df.push()
    return reg.load_balancer

def get_cache_server(config, restart):
    # Explicit restart OR no existing frontier means a fresh registration.
    # Merely relaunching with saved state does not delete the crawl database.
    init_node = Node(
        init, Types=[Register], dataframe=(config.host, config.port))
    return init_node.start(
        config.user_agent, restart or not any(
            os.path.exists(config.save_file + suffix)
            for suffix in ("", ".sqlite3", ".dumb.dat")
        ))
