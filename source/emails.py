import email
import email.mime.application
import imaplib
import logging
import os
import smtplib
from datetime import datetime
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from cryptography.fernet import Fernet
from source.tools import retry

logger = logging.getLogger(__name__)


class Mail:

    def __init__(self, config):
        self.config = config
        self.subject = None
        self.sender = None
        self.attachments = []
        self.saved_attachment = ''

    def __get_password(self):
        key = 'fWn9BDrXryrtcxjXhaO2BR9Oc_bS_zk1k4b6aL_0rbI='
        f = Fernet(key)
        password = f.decrypt(self.config.get('settings', 'password').encode('ascii')).decode('ascii')
        return password

    def __login_to_smtp(self):
        try:
            connect = smtplib.SMTP_SSL(self.config.get('settings', 'smtp_server'),
                                       self.config.getint('settings', 'smtp_port'))
        except Exception as exception:
            logger.error(self.config.get('error', 'server_smtp').format(self.config.get('settings', 'smtp_server'),
                                                                        self.config.get('settings', 'smtp_port')))
            raise exception
        try:
            connect.login(self.config.get('settings', 'bot_account'), self.__get_password())
        except Exception as exception:
            logger.error(self.config.get('error', 'account_login').format(self.config.get('settings', 'bot_account')))
            raise exception
        return connect

    @retry()
    def send(self, recipient, subject, body="", attachments=[], html=True):
        connect = self.__login_to_smtp()
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.config.get('settings', 'bot_account')
        msg['To'] = recipient
        if html:
            txt = MIMEText(body, 'html')
        else:
            txt = MIMEText(body, 'plain')
        msg.attach(txt)

        if len(attachments) > 0:
            for file_path in attachments:
                with open(file_path, 'rb') as fo:
                    file = email.mime.application.MIMEApplication(fo.read())
                file.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                msg.attach(file)
        connect.send_message(msg)
        connect.quit()

    def success_reply(self, output_file, no_processed):
        if no_processed:
            subject = self.config.get('message', 'subject_6')
            messaage_body = self.config.get('message', 'letter_6').format(self.config.get('settings', 'bot_account'))
        else:
            subject = self.config.get('message', 'subject_5')
            messaage_body = self.config.get('message', 'letter_5').format(self.config.get('settings', 'bot_account'))
        attachments = [self.saved_attachment]
        if output_file:
            attachments.append(output_file)
        self.send(recipient=self.sender, subject=subject, body=messaage_body, attachments=attachments)

    def fail_reply(self, reason):
        logger.info(self.config.get('info', 'fail_reply').format(reason))
        subject = self.config.get('message', 'subject_2')
        messaage_body = self.config.get('message', 'letter_2').format(self.config.get('message', reason),
                                                                      self.config.get('settings', 'bot_account'))
        self.send(recipient=self.sender, subject=subject, body=messaage_body)

    def send_fail_to_admin(self, letter, reason=''):
        logger.info(self.config.get('info', 'send_fail'))
        recipient = self.config.get('settings', 'admin')
        if letter == 1:
            subject = self.config.get('message', 'subject_1')
            messaage_body = self.config.get('message', 'letter_1').format(
                self.config.get('message', reason), self.config.get('settings', 'bot_account'))
        elif letter == 3:
            subject = self.config.get('message', 'subject_3')
            messaage_body = self.config.get('message', 'letter_3').format(self.config.get('settings', 'bot_account'))
        elif letter == 4:
            subject = self.config.get('message', 'subject_4')
            messaage_body = self.config.get('message', 'letter_4').format(self.config.get('settings', 'bot_account'))
        self.send(recipient=recipient, subject=subject, body=messaage_body)

    @retry()
    def __login_to_imap(self):
        try:
            mail = imaplib.IMAP4_SSL(self.config.get('settings', 'imap_server'),
                                     self.config.getint('settings', 'imap_port'))
        except Exception:
            logger.error(self.config.get('error', 'server_imap').format(self.config.get('settings', 'imap_server'),
                                                                        self.config.get('settings', 'imap_port')))
        try:
            mail.login(self.config.get('settings', 'bot_account'), self.__get_password())
        except:
            logger.error(self.config.get('error', 'account_login').format(self.config.get('settings', 'bot_account')))
        return mail

    def get_mail_info(self, mail_data):
        for response_part in mail_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_string(response_part[1].decode('utf-8'))
                self.sender = msg['from']
                self.subject = msg['subject']

    def __check_attachments(self):
        attachments = [k[0] for k in self.attachments]
        if len(attachments) == 0:
            logger.warning(self.config.get('warning', 'no_attachments').format(self.subject))
            self.fail_reply(reason='no_attachments')
            raise Exception('*Failed_message')
        if len(attachments) > 1:
            logger.warning(self.config.get('warning', 'more_attachments').format(len(self.attachments), self.subject))
            self.fail_reply(reason='more_attachments')
            raise Exception('*Failed_message')
        if decode_header(attachments[0])[0][1] is not None:
            self.attachments[0] = decode_header(attachments[0])[0][0].decode(decode_header(attachments[0])[0][1])
        if os.path.splitext(attachments[0])[-1].lower() not in self.config.get('settings', 'excel_extensions').split(
                ','):
            logger.warning(self.config.get('warning', 'wrong_extension').format(self.subject, self.attachments[0]))
            self.fail_reply(reason='wrong_extension')
            raise Exception('*Failed_message')

    def __get_attachments(self, mail_data):
        raw_email = mail_data[0][1]
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)
        self.attachments = []
        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            self.attachments.append((part.get_filename(), part))

    @retry()
    def __save_attachment(self):
        if not os.path.exists(self.config.get('path', 'temp')):
            os.mkdir(self.config.get('path', 'temp'))  # Create temp folder
        file_path = os.path.join(self.config.get('path', 'temp'), self.config.get('settings', 'input_file').format(
            datetime.now().strftime(self.config.get('settings', 'file_date_format')),
            os.path.splitext(self.attachments[0][0])[-1]))
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception:
                logger.warning(self.config.get('warning', 'remove_file').format(file_path))
        try:
            with open(file_path, 'wb') as f:
                f.write(self.attachments[0][1].get_payload(decode=True))
            self.saved_attachment = file_path
        except Exception as e:
            logger.warning(self.config.get('error', 'save_attachment').format(file_path))
            raise e

    def save_earlier_mail_attachment(self):
        logger.info(self.config.get('info', 'get_letter'))
        try:
            emails = self.__login_to_imap()
        except Exception as e:
            self.send_fail_to_admin(letter=1, reason='login_exception')
            raise Exception('*Handled_error')
        emails.select(self.config.get('settings', 'mail_box'))
        typ, emails_data = emails.search(None, 'UNSEEN')
        emails_indexes = emails_data[0].split()
        if len(emails_indexes) == 0:
            logger.warning(self.config.get('warning', 'no_letters').format(self.config.get('settings', 'mail_box')))
            self.send_fail_to_admin(letter=1, reason='no_any_letter')
            raise Exception('*Handled_error')
        oldest_mail_idx = emails_indexes[0]
        typ, mail_data = emails.fetch(oldest_mail_idx, '(RFC822)')
        self.get_mail_info(mail_data)
        self.__get_attachments(mail_data)
        self.__check_attachments()
        self.__save_attachment()
