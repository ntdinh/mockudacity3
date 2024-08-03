
import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s', notification_id)

    # TODO: Get connection to database
    conn_string = os.getenv('POSTGRES_CONN_STRING')
    conn = psycopg2.connect(conn_string)

    try:
        # TODO: Get notification message and subject from database using the notification_id
        cur = conn.cursor()
        cur.execute("SELECT message, subject FROM notification WHERE id = %s;", (notification_id,))
        msgContent, subject = cur.fetchone()

        # TODO: Get attendees email and name
        cur.execute("SELECT email FROM attendee;")
        attendees = cur.fetchall()
        logging.info(f'----- {len(attendees)}')

        # TODO: Loop through each attendee and send an email with a personalized subject
        emails = list(map(lambda x: x[0], attendees))
        count = send_email(emails, subject, msgContent)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        total_attendees = f'Notified {count} attendees'
        completed_date = datetime.utcnow()
        cur.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s;", (total_attendees, completed_date, notification_id))
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.close()

def send_email(to_emails, subject, body):
    if to_emails is None or len(to_emails) == 0:
        return

    # [cuongnh] I use Gmail to send emails instead of SendGrid. Currently, it is not possible to register a SendGrid account, some other students also encountered this issue.
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
                logging.info(f'Sent to {email}')
            except Exception as ex1:
                logging.error(f'{str(ex1)} {email}')
    except Exception as ex:
        logging.error(str(ex))
    finally:
        session.quit()

    return count