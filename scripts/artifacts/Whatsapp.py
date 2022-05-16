import sqlite3
import datetime
import xmltodict

from scripts.artifact_report import ArtifactHtmlReport
from scripts.ilapfuncs import logfunc, tsv, timeline, is_platform_windows, open_sqlite_db_readonly

def get_Whatsapp(files_found, report_folder, seeker, wrap_text):

    source_file_msg = ''
    source_file_wa = ''
    whatsapp_msgstore_db = ''
    whatsapp_wa_db = ''
    
    for file_found in files_found:
        
        file_name = str(file_found)
        if file_name.endswith('msgstore.db'):
           whatsapp_msgstore_db = str(file_found)
           source_file_msg = file_found.replace(seeker.directory, '')

        if file_name.endswith('wa.db'):
           whatsapp_wa_db = str(file_found)
           source_file_wa = file_found.replace(seeker.directory, '')

    db = open_sqlite_db_readonly(whatsapp_msgstore_db)
    cursor = db.cursor()
    try:
        cursor.execute('''
        SELECT case CL.video_call when 1 then "Video Call" else "Audio Call" end as call_type, 
               CL.timestamp/1000 as start_time, 
               ((cl.timestamp/1000) + CL.duration) as end_time, 
               case CL.from_me when 0 then "Incoming" else "Outgoing" end as call_direction,
		       J1.raw_string AS from_id,
                            group_concat(J.raw_string) AS group_members
                     FROM   call_log_participant_v2 AS CLP
                            JOIN call_log AS CL
                              ON CL._id = CLP.call_log_row_id
                            JOIN jid AS J
                              ON J._id = CLP.jid_row_id
                            JOIN jid as J1
                              ON J1._id = CL.jid_row_id
                            GROUP  BY CL._id
        ''')

        all_rows = cursor.fetchall()
        usageentries = len(all_rows)
    except:
        usageentries = 0
        
    if usageentries > 0:
        report = ArtifactHtmlReport('Whatsapp - Group Call Logs')
        report.start_artifact_report(report_folder, 'Whatsapp - Group Call Logs')
        report.add_script()
        data_headers = ('Start Time', 'End Time','Call Type', 'Call Direction', 'From ID', 'Group Members') # Don't remove the comma, that is required to make this a tuple as there is only 1 element
        data_list = []
        for row in all_rows:
            starttime = datetime.datetime.fromtimestamp(int(row[1])).strftime('%Y-%m-%d %H:%M:%S')
            endtime = datetime.datetime.fromtimestamp(int(row[2])).strftime('%Y-%m-%d %H:%M:%S')
            data_list.append(( starttime, endtime, row[0], row[3], row[4], row[5]))

        report.write_artifact_data_table(data_headers, data_list, whatsapp_msgstore_db)
        report.end_artifact_report()
        
        tsvname = f'Whatsapp - Group Call Logs'
        tsv(report_folder, data_headers, data_list, tsvname, source_file_msg)

        tlactivity = f'Whatsapp - Group Call Logs'
        timeline(report_folder, tlactivity, data_list, data_headers)
        
    else:
        logfunc('No Whatsapp Group Call Logs found')
        
    try:        
        cursor.execute('''
                     SELECT CL.timestamp/1000 as start_time, 
                            case CL.video_call when 1 then "Video Call" else "Audio Call" end as call_type, 
                            ((CL.timestamp/1000) + CL.duration) as end_time, 
                            J.raw_string AS num, 
                            case CL.from_me when 0 then "Incoming" else "Outgoing" end as call_direction
                     FROM   call_log AS CL 
                            JOIN jid AS J 
                              ON J._id = CL.jid_row_id 
                     WHERE  CL._id NOT IN (SELECT DISTINCT call_log_row_id 
                                           FROM   call_log_participant_v2) 
        ''')
        
        all_rows = cursor.fetchall()
        usageentries = len(all_rows)
    except:
        usageentries = 0
        
    if usageentries > 0:
        report = ArtifactHtmlReport('Whatsapp - Single Call Logs')
        report.start_artifact_report(report_folder, 'Whatsapp - Single Call Logs')
        report.add_script()
        data_headers = ('Start Time','Call Type','End Time','Number','Call Direction') # Don't remove the comma, that is required to make this a tuple as there is only 1 element
        data_list = []
        for row in all_rows:
            starttime = datetime.datetime.fromtimestamp(int(row[0])).strftime('%Y-%m-%d %H:%M:%S')
            endtime = datetime.datetime.fromtimestamp(int(row[2])).strftime('%Y-%m-%d %H:%M:%S')
            data_list.append((starttime, row[1], endtime, row[3], row[4]))
            
        report.write_artifact_data_table(data_headers, data_list, whatsapp_msgstore_db)
        report.end_artifact_report()
        
        tsvname = f'Whatsapp - Single Call Logs'
        tsv(report_folder, data_headers, data_list, tsvname, source_file_msg)
        
        tlactivity = f'Whatsapp - Single Call Logs'
        timeline(report_folder, tlactivity, data_list, data_headers)
        
    else:
        logfunc('No Whatsapp Single Call Log available')
            
    cursor.execute('''attach database "''' + whatsapp_wa_db + '''" as wadb ''')
    
    
    try:
        cursor.execute('''
                    SELECT 	MESSAGES._id AS ID, 
        MESSAGES.from_me AS DIRECTION,
		CASE
			WHEN MESSAGES.from_me == 0 AND RECEPIENT LIKE '%@g.us%' THEN	(SELECT raw_string FROM jid WHERE jid._id == MESSAGES.sender_jid_row_id)
			WHEN MESSAGES.from_me == 0 THEN RECEPIENT
			ELSE "user"
		END AS SENDER,
		MESSAGES.timestamp/1000 AS SEND_TIMESTAMP, 
        MESSAGES.received_timestamp/1000 AS RECEIVED_TIMESTAMP,
		CASE
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 11 THEN "Group created: " || MESSAGES.text_data
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 67 THEN "The messages in this chat are crypto protected"
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 56 THEN "Temp messages configuration changed"
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 12 THEN "Added " || (SELECT raw_string FROM (SELECT message_system_chat_participant.message_row_id, raw_string FROM jid JOIN message_system_chat_participant ON jid._id = message_system_chat_participant.user_jid_row_id) WHERE message_row_id == MESSAGES._id) 
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 14 THEN "Removed " || (SELECT raw_string FROM (SELECT message_system_chat_participant.message_row_id, raw_string FROM jid JOIN message_system_chat_participant ON jid._id = message_system_chat_participant.user_jid_row_id) WHERE message_row_id == MESSAGES._id) 
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 56 THEN "call"
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 70 THEN "call lost"
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 58 THEN CASE WHEN MESSAGES.text_data LIKE 'True' THEN "You has blocked this contact" ELSE "You has unblocked this contact" END
			WHEN MESSAGES.message_type == 7 AND MESSAGES.action_type == 69 THEN "This company work with other to manage this chat"
			ELSE MESSAGES.text_data
		END	AS CONTENT,
		CASE
			WHEN MESSAGES.lookup_tables == 2 OR MESSAGES.lookup_tables == 3 THEN
				(SELECT CITED_ID FROM (SELECT message_quoted.message_row_id, message_quoted.key_id AS CITED_KEY_ID, message._id AS CITED_ID FROM message_quoted JOIN message ON CITED_KEY_ID == message.key_id) WHERE message_row_id == MESSAGES._id)
			ELSE ""
			END AS QUOTED_MESSAGE_ID,
		RECEPIENT,
		CASE
		WHEN MESSAGES.message_type == 1 OR MESSAGES.message_type == 20 OR MESSAGES.message_type == 9 OR MESSAGES.message_type == 2 OR MESSAGES.message_type == 3 THEN (SELECT file_path FROM message_media WHERE message_media.message_row_id == MESSAGES._id)
		WHEN MESSAGES.message_type == 4 THEN (SELECT vcard FROM message_vcard WHERE message_vcard.message_row_id == MESSAGES._id)
		WHEN MESSAGES.message_type == 5 THEN (SELECT latitude || ';' || longitude FROM message_location WHERE message_location.message_row_id == MESSAGES._id)
		ELSE ""
		END AS ATTACHMENT
	FROM (SELECT *
				FROM message
				LEFT JOIN message_system
				ON 	message._id = message_system.message_row_id ) AS MESSAGES
	JOIN (
		SELECT jid._id AS CONTACT_ID, raw_string AS RECEPIENT, chat._id AS CHAT_ROW
			FROM jid
			JOIN chat
			ON jid._id = chat.jid_row_id)
	ON 	MESSAGES.chat_row_id = CHAT_ROW 
	ORDER BY RECEPIENT
        ''')
        
        all_rows = cursor.fetchall()
        usageentries = len(all_rows)
    except:
        usageentries = 0
        
    if usageentries > 0:
        report = ArtifactHtmlReport('Whatsapp - Messages')
        report.start_artifact_report(report_folder, 'Whatsapp - Messages')
        report.add_script()
        data_headers = ('Message ID','Direction','Sender','Send Timestamp', 'Received Timestamp','Content','Message Quoted','Recipient','Attachment') # Don't remove the comma, that is required to make this a tuple as there is only 1 element
        data_list = []
        for row in all_rows:
            sendtime = datetime.datetime.fromtimestamp(int(row[3])).strftime('%Y-%m-%d %H:%M:%S')
            if row[4] == 0:
                receivetime = "";
            else:
                receivetime = datetime.datetime.fromtimestamp(int(row[4])).strftime('%Y-%m-%d %H:%M:%S')
            
            data_list.append((row[0], row[1], row[2], sendtime, receivetime, row[5], row[6], row[7], row[8]))
            
        report.write_artifact_data_table(data_headers, data_list, whatsapp_msgstore_db)
        report.end_artifact_report()
        
        tsvname = f'Whatsapp - Messages'
        tsv(report_folder, data_headers, data_list, tsvname, source_file_msg)
        
        tlactivity = f'Whatsapp - Messages'
        timeline(report_folder, tlactivity, data_list, data_headers)
        
    else:
        logfunc('No Whatsapp messages data available')
        
    

    db.close()

    db = open_sqlite_db_readonly(whatsapp_wa_db)
    cursor = db.cursor()
    try:
        cursor.execute('''
                     SELECT jid, 
                            CASE 
                              WHEN WC.number IS NULL THEN WC.jid 
                              WHEN WC.number == "" THEN WC.jid 
                              ELSE WC.number 
                            END number, 
                            CASE 
                              WHEN WC.given_name IS NULL 
                                   AND WC.family_name IS NULL 
                                   AND WC.display_name IS NULL THEN WC.jid 
                              WHEN WC.given_name IS NULL 
                                   AND WC.family_name IS NULL THEN WC.display_name 
                              WHEN WC.given_name IS NULL THEN WC.family_name 
                              WHEN WC.family_name IS NULL THEN WC.given_name 
                              ELSE WC.given_name 
                                   || " " 
                                   || WC.family_name 
                            END name 
                     FROM   wa_contacts AS WC
        ''')

        all_rows = cursor.fetchall()
        usageentries = len(all_rows)
    except:
        usageentries = 0
        
    if usageentries > 0:
        report = ArtifactHtmlReport('Whatsapp - Contacts')
        report.start_artifact_report(report_folder, 'Whatsapp - Contacts')
        report.add_script()
        data_headers = ('Number','Name') # Don't remove the comma, that is required to make this a tuple as there is only 1 element
        data_list = []
        for row in all_rows:
            data_list.append((row[0], row[1]))

        report.write_artifact_data_table(data_headers, data_list, whatsapp_wa_db)
        report.end_artifact_report()
        
        tsvname = f'Whatsapp - Contacts'
        tsv(report_folder, data_headers, data_list, tsvname, source_file_wa)

    else:
        logfunc('No Whatsapp Contacts found')

    db.close

    for file_found in files_found:
        if('com.whatsapp_preferences_light.xml' in file_found):
            with open(file_found, encoding='utf-8') as fd:
                xml_dict = xmltodict.parse(fd.read())
                string_dict = xml_dict.get('map','').get('string','')
                data = []
                for i in range(len(string_dict)):
                    if(string_dict[i]['@name'] == 'push_name'):                 # User Profile Name
                        data.append(string_dict[i]['#text'])
                    if(string_dict[i]['@name'] == 'my_current_status'):         # User Current Status
                        data.append(string_dict[i]['#text'])
                    if(string_dict[i]['@name'] == 'version'):                   # User current whatsapp version
                        data.append(string_dict[i]['#text'])
                    if(string_dict[i]['@name'] == 'ph'):                        # User Mobile Number
                        data.append(string_dict[i]['#text'])
                    if(string_dict[i]['@name'] == 'cc'):                        # User country code
                        data.append(string_dict[i]['#text'])

                if(len(data)>0):
                    report = ArtifactHtmlReport('Whatsapp - User Profile')
                    report.start_artifact_report(report_folder,'Whatsapp - User Profile')
                    report.add_script()
                    data_headers = ('Version', 'Name', 'User Status', 'Country Code', 'Mobile Number')
                    data_list = []
                    data_list.append((data[1], data[4], data[2], data[3], data[0]))
                    report.write_artifact_data_table(data_headers, data_list, file_found, html_escape=False)
                    report.end_artifact_report()

                    tsvname = "Whatsapp - User Profile"
                    tsv(report_folder, data_headers, data_list,tsvname)

                    tlactivity = "Whatsapp - User Profile"
                    timeline(report_folder, tlactivity, data_list, data_headers)
                else:
                    logfunc("No Whatsapp - Profile data found")
    return
