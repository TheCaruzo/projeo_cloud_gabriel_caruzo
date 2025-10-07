from datetime import datetime, timedelta

def yymmdd():
    ontem = datetime.now() - timedelta(days=1)
    return ontem.strftime("%y%m%d")