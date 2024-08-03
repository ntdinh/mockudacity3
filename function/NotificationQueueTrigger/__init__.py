import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))

    # TODO: Get connection to database
    conn_string = os.getenv('DB_URL')
    conection = psycopg2.connect(conn_string)

    try:
        cur = conection.cursor()
        cur.execute("select message, subject from notification where id = %s;", (notification_id,))
        msgContent, subject = cur.fetchone()

        # TODO: Get attendees email and name
        cur.execute("select email from attendee")
        attendees = cur.fetchall()
        logging.info(f'----- {len(attendees)}')

        emails = list(map(lambda x: x[0], attendees))
        count = send_email(emails, subject, msgContent)

        total_attendees = f'Notification {count}'
        completed_date = datetime.utcnow()
        cur.execute("update notification set status = %s, completed_date = %s where id = %s;", (total_attendees, completed_date, notification_id))
        conection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conection.close()

def send_email(to_emails, subject, body):
    if to_emails is None or len(to_emails) == 0:
        return


    # I use Gmail to send emails instead of SendGrid. SendGrid seems to have an issue and I can't get it to work.
    mail_server = 'smtp.gmail.com'
    port = 465
    sender = os.getenv('SENDER_EMAIL')
    password = os.getenv('SENDER_PWD')

    session = smtplib.SMTP_SSL(mail_server, port)
    count = 0
    try:
        session.login(sender, password)
        for email in to_emails:
            try:
                msg = MIMEText(body)
                msg['Subject'] = subject
                msg['From'] = sender
                msg['To'] = email
                session.sendmail(sender, email, msg.as_string())
                count += 1
            except Exception as ex1:
                logging.error(f'{str(ex1)} {email}')
    except Exception as ex:
        logging.error(str(ex))
    finally:
        session.quit()

    return count