#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import imaplib
import mysql.connector
import email
import re
import sys
import logging
from logging.handlers import RotatingFileHandler

###función que permite insertar los datos de los correos en la bae de datos###
def insert_db(registers):
    miConexion = mysql.connector.connect( host='localhost', user='root', passwd='debian', db='MELI')
    cur = miConexion.cursor()
    for mail in registers:
        cur.execute("INSERT INTO mails (received_dt, sender, subject) VALUES (\"{}\", \"{}\", \"{}\", "
                    "\"{}\")".format(mail["date"], mail["from"], mail["subject"])
                    )
    miConexion.commit()
    cur.close()
    miConexion.close()

### Mail connection settings ###
imapServer = 'imap.gmail.com'
user = sys.argv[1]
passwd = sys.argv[2]
###Ruta donde se almacenaran los logs ###
logsPath = '/home/debian/getMail.log'
### Manejo de Logs: Formato, fecha, debug, y rotacion ###
Rthandler = RotatingFileHandler(logsPath, maxBytes=1024 * 1024,
                                backupCount=3)
formatter = \
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                      datefmt='%d-%b-%y %H:%M:%S')
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)
logging.getLogger('').setLevel(logging.DEBUG)
### Unique function for mail search.###
def searchMail():
    # ## Search for unread emails###
    mailBox.select()
    (result, data) = mailBox.uid('search', None, '(UNSEEN)')
    # ## If you meet the condition, that is, there is mail to read.###
    if data[0] == '':
        logging.warning('No hay correos nuevos por leer')
    else:
        logging.info('Hay un correo nuevo')
    ids = data[0]
    id_list = ids.split()
   ### emails enumeration is performed###
    i = len(id_list)
    all_mails = list()
    ###read the emails one by one#
    for x in range(i):
        latest_email_uid = id_list[x]
        (result, email_data) = mailBox.uid('fetch', latest_email_uid,
                '(RFC822)')
        raw_email = email_data[0][1]
        raw_email_string = raw_email
        email_message = email.message_from_bytes(raw_email_string)
        parsed_dict = {
        'from': email.utils.parseaddr(email_message['From']),
        'date': "",
        'to': email_message['to'],
        'subject': email_message['Subject'],
        'body_plain': '',
        'body_html': '',
        }
        #### read the body of the email#
        for part in email_message.walk():  # iterates trough all parts of the email
            if part.get_content_type() == 'text/plain':  # save plain text email body
                parsed_dict['body_plain'] = part.get_payload(decode=True).decode('utf-8')
            elif part.get_content_type() == 'text/html':  # save html email body
                parsed_dict['body_html'] = part.get_payload(decode=True).decode('utf-8')
        if "devops" not in parsed_dict['body_plain'].lower():###if it does not meet the condition, the cycle ends###
            continue
  ###date format conversion###
        mail_date = email_message['Date']
        logging.info(mail_date)
        mail_date = re.sub(r'(.*)\s\-?\w+', r"\g<1>", mail_date)
        mail_date = datetime.datetime.strptime(mail_date, "%a, %d %b %Y %H:%M:%S")
        mail_date = datetime.datetime.isoformat(mail_date)
        parsed_dict["date"] = mail_date
        logging.info(parsed_dict["date"])
        all_mails.append(parsed_dict)

###call function insert_db###
    insert_db(all_mails)
##### Script start #####
logging.info('Inicio de la rutina')
### mailbox authentication###
try:
    mailBox = imaplib.IMAP4_SSL(imapServer)
    mailBox.login(user, passwd)
    mailBox.select(readonly=True)
    logging.info('Autenticacion Exitosa')
except:
    logging.error('Autenticacion Fallida')
    logging.error('Fin de la rutina')
    sys.exit(0)

searchMail()
mailBox.close()
mailBox.logout()
logging.info('Fin de la rutina')
