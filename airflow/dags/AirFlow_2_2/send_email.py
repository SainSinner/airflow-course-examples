from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib


def html_pretty(df) -> str:
    """ Pretty html dataframe
    """
    return """\
    <html>
        <head></head>
        <body>
            {0}
        </body>
    </html>
    """.format(df.to_html())


def _send_email(data, username, password, host, port, to, From) -> None:
    """ Send DF to email
    """

    msg = MIMEMultipart()
    part = MIMEText(html_pretty(data), 'html')
    msg.attach(part)

    server = smtplib.SMTP(host, port)
    server.starttls()
    server.login(username, password)
    server.sendmail(From, to, msg.as_string())
    server.quit()


