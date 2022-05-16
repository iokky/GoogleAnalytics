from apscheduler.schedulers.background import BackgroundScheduler

from connectors.google_analytics.client import GoogleAnalyticsTransactionsConn, GoogleAnalyticsUsersConn

from logger.telegram import send_message

google_analytics_users_agent = GoogleAnalyticsUsersConn()
google_analytics_transaction_agent = GoogleAnalyticsTransactionsConn()


def ga_users_run():
    google_analytics_users_agent.run()
    send_message('google_analytics_users_agent done')


def ga_transaction_run():
    google_analytics_transaction_agent.run()
    send_message('google_analytics_users_agent done')


scheduler = BackgroundScheduler({'apscheduler.timezone': 'Europe/Moscow'})
scheduler.start()
scheduler.add_job(ga_users_run, 'cron', hour='01', minute='10')
scheduler.add_job(ga_transaction_run, 'cron', hour='01', minute='30')

