import psutil
class Status:
    busy = "b"
    idle = "i"
    terminated = "t"
    new = "n"

    map_shortcode_to_longcode = {"b": "busy", "i": "idle", "t": "terminated",
                                 "n": "new"}

    unhealthy_status = {
        psutil.STATUS_ZOMBIE,
        psutil.STATUS_DEAD,
        psutil.STATUS_LOCKED,
        psutil.STATUS_STOPPED
    }

    @classmethod
    def shortcode_to_longcode(cls, short_code):
        return cls.map_shortcode_to_longcode[short_code]