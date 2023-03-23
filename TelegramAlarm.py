import telegram
api_key = '5572934553:AAHqkgRBSvF_4GQUSSEA85vrajtO-ESQeIo'
users_id = ['93492210']#, '1579982009', '939848799']
bot = telegram.Bot(token=api_key)
message = "Parisa Khar ast"


for chadID in users_id:
    bot.send_message(chat_id=chadID, text=message)