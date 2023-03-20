import os
import requests
# import telebot
import json
import openpyxl
import sqlite3
import matplotlib
import traceback
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.linear_model import LinearRegression
from telebot import types
from telebot import TeleBot
from datetime import datetime

print("Бот запущен")

matplotlib.use('Agg')

API_TOKEN = 'TQUFAOQ0L3FZGWJ1'

TOKEN = '5613398788:AAEE4hXNRD7fG2TgEJMSNcoOY7B5fhVjC2c'

bot = TeleBot(TOKEN)

try:
    sqlite_connection = sqlite3.connect('stocks.db', check_same_thread=False)
    cursor = sqlite_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables_list = list(map(lambda tuple_obj: tuple_obj[0], cursor.fetchall()))
    # table_is_empty = {"by_month": False, "by_week": False, "by_day":False}
    for table_name in ['by_month', 'by_week', 'by_day']:
        if table_name not in tables_list:
            cursor.execute(
                f"CREATE TABLE {table_name} (first_currency TEXT, second_currency TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS indx ON {table_name} (first_currency, second_currency)")
            # table_is_empty[f'{table_name}'] = True
        # else:
        # cursor.execute(f"SELECT count(*) FROM {table_name}")
        # table_is_empty[f'{table_name}'] = cursor.fetchall()[0][0] is 0
    sqlite_connection.commit()

except sqlite3.Error as error:
    print("Ошибка при подключении к БД", error)

stock_data_json = None


# def tableIsEmpty(table_name: str, cursor, connection) -> bool:
#     try:
#         cursor.execute(f"SELECT count(*) FROM {table_name}")
#         return cursor.fetchall()[0][0] == 0
#     except sqlite3.OperationalError:
#         cursor.execute(f"CREATE TABLE {table_name} (first_currency TEXT, second_currency TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL)")
#     connection.commit()
#     return True


def dataOfCurrenciesExist(table_name: str, first_currency: str, second_currency: str, cursor, connection) -> bool:
    cursor.execute(
        f"SELECT count(*) FROM {table_name} WHERE first_currency = '{first_currency}' AND second_currency = '{second_currency}'")
    result = cursor.fetchall()[0][0]
    print(f"result after exist check:\n{result}")
    return result


@bot.message_handler(commands=["start"])
def start(message, k=0):
    if k == 0:
        if message.from_user.first_name != None:
            msg = "Привет, {}, выбери подходящий тебе анализ биржы. Подробнее о них - в команде /help".format(
                message.from_user.first_name)
        elif message.from_user.last_name != None:
            msg = "Привет, {}, выбери подходящий тебе анализ биржы. Подробнее о них - в команде /help".format(
                message.from_user.last_name)
        else:
            msg = "Привет, {}, выбери подходящий тебе анализ биржы. Подробнее о них - в команде /help".format(
                message.from_user.username)
    else:
        msg = "Выбери подходящий тебе анализ биржы. Подробнее о них - в команде /help"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Первичный анализ", callback_data='analytics'))
    markup.add(types.InlineKeyboardButton("Графический анализ", callback_data='graphics'))
    markup.add(types.InlineKeyboardButton("Статистический анализ", callback_data='stats'))
    bot.send_message(message.chat.id, msg, parse_mode="html", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: True)
def answer(call):
    global stock_data_json
    if call.data == 'analytics':
        mesg = bot.send_message(call.message.chat.id,
                                "Вы выбрали первичный анализ. Введите исследуемую пару валют в формате X-X (например:USD-RUB):",
                                parse_mode='html')
        bot.register_next_step_handler(mesg, step_1_to_analytics)

    elif call.data == 'graphics':
        mesg = bot.send_message(call.message.chat.id,
                                "Вы выбрали графисеский анализ. Введите исследуемую пару валют в формате X-X (например:USD-RUB):",
                                parse_mode='html')
        bot.register_next_step_handler(mesg, graphics)


    elif call.data == 'stats':
        mesg = bot.send_message(call.message.chat.id,
                                "Вы выбрали статистический анализ. Введите исследуемую пару валют в формате X-X (например:USD-RUB):",
                                parse_mode='html')
        bot.register_next_step_handler(mesg, get_time_to_stats)

    elif ('FX_DAILY' in call.data) or ('FX_MONTHLY' in call.data) or ('FX_WEEKLY' in call.data):
        res = stats(call.data)
        if len(res) > 4096:
            for x in range(0, len(res), 4096):
                bot.send_message(call.message.chat.id, res[x:x + 4096], parse_mode='html')
        else:
            bot.send_message(call.message.chat.id, res, parse_mode='html')
        return start(call.message, 1)


    elif 'ANALYTICS' in call.data:
        step_2_to_analytics(call)

    elif 'to_json' in call.data:
        createJsonFile(stock_data_json, "data")
        bot.send_document(call.message.chat.id, open("data.json", 'rb'))
        os.remove('data.json')


    elif "to_csv" in call.data:
        createCSVFile(stock_data_json, "data")
        bot.send_document(call.message.chat.id, open("data.csv", 'rb'))
        os.remove('data.csv')

    elif "to_excel" in call.data:
        createExcelFile(stock_data_json, "data")
        bot.send_document(call.message.chat.id, open("data.xlsx", 'rb'))
        os.remove('data.xlsx')


@bot.message_handler(commands=["help"])
def help(message):
    msg = "Бот умеет предоставлять информацию в разных видах на выбор: первичный, графический и статистический анализы.\n" \
          "\n<b>1.Первчиный</b> - вывод курса валют (close, open, low, high) за определённый промежуток времени, указанный в форматном виде.\n" \
          "\n<b>2.Графический</b>  - построение графиков распределения и рассеивания по параметрам курса валют за всё время время.\n" \
          "\n<b>3.Статистический</b> - вывод математических статистик по параметрам курса валют за определённый промежуток времени."
    bot.send_message(message.chat.id, msg, parse_mode='html')
    bot.send_message(message.chat.id, "При указании валют - указывать их код. Достпуны следующие валюты:")
    values_msg = ''
    values = pd.read_csv('physical_currency_list.csv')
    di = values.to_dict('list')
    for i in range(len(di['currency code'])):
        values_msg += '{} - {}\n'.format(di['currency code'][i], di['currency name'][i])
    if len(values_msg) > 4096:
        for x in range(0, len(values_msg), 4069):
            bot.send_message(message.chat.id, values_msg[x:x + 4069], parse_mode='html')
    else:
        bot.send_message(message.chat.id, values_msg, parse_mode='html')

    return start(message, 1)


def step_1_to_analytics(message):
    txt = message.text
    arr = txt.split()
    query_array = []
    for i in arr:
        query_array.append(i.split('-'))
        # print(f"{i} Querry_array is:  ",query_array)
    currnecy_pair = query_array[0]
    # print("step1: ", currnecy_pair)
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(
        "По дням",
        callback_data=f'ANALYTICS_DAYS_{currnecy_pair[0]}_{currnecy_pair[1]}'))
    markup.add(types.InlineKeyboardButton(
        "По неделям",
        callback_data=f'ANALYTICS_WEEKS_{currnecy_pair[0]}_{currnecy_pair[1]}'))
    markup.add(types.InlineKeyboardButton(
        "По месяцам",
        callback_data=f'ANALYTICS_MONTHS_{currnecy_pair[0]}_{currnecy_pair[1]}'))
    bot.send_message(message.chat.id, "Выберите размах исследуемого периода.", reply_markup=markup)


def step_2_to_analytics(call):
    mesg = bot.send_message(call.message.chat.id, "Введите период в формате YYYY-MM-DD : YYYY-MM-DD.",
                            parse_mode='html')
    bot.register_next_step_handler(mesg, step_3_to_analytics, call.data)


def step_3_to_analytics(message, callback_data):
    mesg = bot.send_message(message.chat.id,
                            "Введите количество записей, которое хотите просмотреть (максимальное - 20):",
                            parse_mode='html')
    bot.register_next_step_handler(mesg, final_step_to_analytics, callback_data, message.text.replace(' ', ''))


def final_step_to_analytics(message, callback_data, period_str):
    global stock_data_json
    max_limit = 20
    limit = int(message.text)
    limit = limit if limit <= max_limit else max_limit
    callback_data = callback_data.split('_')
    currency_pair = list(map(lambda string: string.upper(), callback_data[2:]))
    period = period_str.split(':')

    if callback_data[1] == "DAYS":
        gap_function = "FX_DAILY"
        table_name = 'by_day'  # + f'_{currency_pair[0]}_{currency_pair[1]}'
        data_key = 'Daily'
        word = "дням"
        # exist_in_DB = not table_is_empty[table_name]
    elif callback_data[1] == "WEEKS":
        gap_function = "FX_WEEKLY"
        table_name = 'by_week'  # + f'_{currency_pair[0]}_{currency_pair[1]}'
        data_key = 'Weekly'
        word = "неделям"
        # exist_in_DB = not table_is_empty[table_name]
    else:
        gap_function = "FX_MONTHLY"
        table_name = 'by_month'  # + f'_{currency_pair[0]}_{currency_pair[1]}'
        data_key = 'Monthly'
        word = "месяцам"

    # exist_in_DB = not tableIsEmpty(table_name, cursor, sqlite_connection)
    exist_in_DB = dataOfCurrenciesExist(table_name, currency_pair[0], currency_pair[1], cursor, sqlite_connection)
    print(f"exist in DB : {exist_in_DB}")

    msg = ''
    records_count = 0
    date1 = datetime.strptime(period[0], "%Y-%m-%d")
    date2 = datetime.strptime(period[1], "%Y-%m-%d")
    if exist_in_DB:
        msg += "[from the DB]\n\n"
        cursor.execute(
            f"SELECT date, open, high, low, close FROM {table_name} WHERE DATE(date) >= DATE('{period[0]}') AND DATE(date) <= DATE('{period[1]}') " \
            f"AND first_currency = '{currency_pair[0]}' AND second_currency = '{currency_pair[1]}'")
        select = cursor.fetchall()
        stock_data_json = {}
        for date, open, high, low, close in select:
            stock_data_json[date] = {"1. open": open, "2. high": high, "3. low": low, "4. close": close}
            if records_count < limit:
                msg += f"<b>{date}:</b>\n"
                msg += f"open: {open}\n" \
                       f"high: {high}\n" \
                       f"low: {low}\n" \
                       f"close: {close}\n\n"
            records_count += 1
        # print("from the bd\n", json.dumps(stock_data_json,indent= 4))
    else:
        msg += "[from the parser]\n\n"
        url = f'https://www.alphavantage.co/query?function={gap_function}&from_symbol={currency_pair[0].upper()}&to_symbol={currency_pair[1].upper()}&apikey={API_TOKEN}'
        r = requests.get(url)
        stock_data_json_full = r.json()
        print(f"[from the parser] [{currency_pair[0]}/{currency_pair[1]}]\n\n",
              json.dumps(stock_data_json_full, indent=4))
        try:
            stock_data_json = stock_data_json_full[f'Time Series FX ({data_key})']
        except KeyError:
            bot.send_message(message.chat.id, "Произошла ошибка: к сожалению, на сервере нет запрашиваемых данных.")
            return
        new_stock_data_json = {}
        for date, records in stock_data_json.items():
            cursor.execute(
                f"INSERT INTO {table_name} VALUES('{currency_pair[0]}', '{currency_pair[1]}', '{date}',{records['1. open']}, {records['2. high']}, {records['3. low']}, {records['4. close']})")
            if date1 <= datetime.strptime(date, "%Y-%m-%d") <= date2:
                open = records['1. open']
                high = records['2. high']
                low = records['3. low']
                close = records['4. close']
                new_stock_data_json[date] = {"1. open": open, "2. high": high, "3. low": low, "4. close": close}
                if records_count < limit:
                    msg += f"<b>{date}:</b>\n"
                    msg += f"open: {open}\n" \
                           f"high: {high}\n" \
                           f"low: {low}\n" \
                           f"close: {close}\n\n"
                records_count += 1
        # table_is_empty[table_name] = False
        sqlite_connection.commit()
        stock_data_json = new_stock_data_json
        # print(msg)
    msg = (
            f'Первые {limit if limit < records_count else records_count} записей по {word} за указанный период [{currency_pair[0].upper()} - {currency_pair[1].upper()}].'
            + f"Записей всего {records_count}:\n\n" + msg
            + f"\nПолные данные можно импортировать с помощью кнопок ниже:")
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Импортировать данные в формате csv", callback_data='to_csv'))
    markup.add(types.InlineKeyboardButton("Импортировать данные в формате json", callback_data='to_json'))
    markup.add(types.InlineKeyboardButton("Импортировать данные в формате excel", callback_data='to_excel'))
    bot.send_message(message.chat.id, msg, reply_markup=markup, parse_mode='html')
    return start(message, 1)
    # print(json.dumps(stock_data_json, indent= 4))


def createJsonFile(obj: dict, filename: str = 'file'):
    with open(f'{filename}.json', 'w') as fp:
        json.dump(obj, fp, indent=4)


def createCSVFile(obj: dict, filename: str = 'file'):
    df = pd.DataFrame.from_dict(obj)
    df.to_csv(f'{filename}.csv', index=False, header=True)


def createExcelFile(obj: dict, filename: str = 'file'):
    new_list = []
    for key, value in obj.items():
        new_record = {"date": key}
        new_record.update(value)
        new_list.append(new_record)
    df = pd.DataFrame(data=new_list)
    df.to_excel(f"{filename}.xlsx", index=False)


def trade(x, slope, intercept):
    return slope * x + intercept


def graphics(message):
    txt = message.text
    query_array = txt.split('-')
    url = 'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={}&to_symbol={}&apikey={}'.format(
        query_array[0], query_array[1], API_TOKEN)
    json_request = requests.get(url).json()
    data = pd.DataFrame.from_dict(json_request['Time Series FX (Daily)']).T
    data = data.astype(float)
    if len(data) < 10:
        ans = "Из-за маленького количества данных линии тренда могут не совпадать с действительностью."
    else:
        ans = 'Достаточное количество исследуемых данных для построения графиков.'
    bot.send_message(message.chat.id, "Количество торговых дней - {}. {}".format(len(data), ans))
    data['timp'] = list(range(len(data)))
    data['scaled_timp'] = (data['timp'] - np.mean(data['timp'])) / np.std(data['timp'])

    # plot + trand

    lr = LinearRegression()
    lr.fit(data['scaled_timp'].values.reshape(-1, 1), data['1. open'])
    if lr.coef_ < 0:
        trand_det = 'Убываюший'
    else:
        trand_det = "Восходящий"
    plt.ion()
    plt.clf()
    plt.grid()
    plt.plot(data['timp'], data['1. open'], label="Open")
    plt.plot(data['timp'], data['4. close'], label="Close")
    plt.plot(data['timp'], trade(data['scaled_timp'], lr.coef_, lr.intercept_), label="{} тренд".format(trand_det))
    plt.title("Изменение соотношение пары {} за последнее время".format(txt))
    plt.xlabel("Масштабированный таймстеп")
    plt.ylabel("Соотношение {}".format(txt))
    plt.legend(loc="lower left")
    file_name = 'Change of open close {}.png'.format(txt)
    plt.savefig(file_name)
    bot.send_message(message.chat.id, "График соотношения по Open/Close:")
    bot.send_photo(message.chat.id, photo=open(file_name, 'rb'))
    os.remove(file_name)
    lr.fit(data['scaled_timp'].values.reshape(-1, 1), data['2. high'])
    if lr.coef_ < 0:
        trand_det = 'Убываюший'
    else:
        trand_det = "Восходящий"
    plt.clf()
    plt.grid()
    plt.plot(data['timp'], data['2. high'], label="High")
    plt.plot(data['timp'], data['3. low'], label="Low")
    plt.plot(data['timp'], trade(data['scaled_timp'], lr.coef_, lr.intercept_), label="{} тренд".format(trand_det))
    plt.title("Изменение соотношение пары {} за последние время".format(txt))
    plt.xlabel("Масштабированный таймстеп")
    plt.ylabel("Соотношение {}".format(txt))
    plt.legend(loc="lower left")
    file_name = 'Change of high low {}.png'.format(txt)
    plt.savefig(file_name)
    bot.send_message(message.chat.id, "График соотношения по High/Low:")
    bot.send_photo(message.chat.id, photo=open(file_name, 'rb'))
    os.remove(file_name)

    # boxplot
    plt.clf()
    df = data.drop(['timp', 'scaled_timp'], axis=1)
    df.boxplot()
    plt.title("Распределение пары {} по значениям".format(txt))
    plt.ylabel("Соотношение {}".format(txt))
    file_name = 'Boxplot for {}.png'.format(txt)
    plt.savefig(file_name)
    bot.send_message(message.chat.id, "График распределения:")
    bot.send_photo(message.chat.id, photo=open(file_name, 'rb'))
    os.remove(file_name)

    return start(message, 1)


def get_time_to_stats(message):
    msg = "Выберите размах исследуемого периода."
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("По дням", callback_data='FX_DAILY {}'.format(message.text)))
    markup.add(types.InlineKeyboardButton("По неделям", callback_data='FX_WEEKLY {}'.format(message.text)))
    markup.add(types.InlineKeyboardButton("По месяцам", callback_data='FX_MONTHLY {}'.format(message.text)))
    bot.send_message(message.chat.id, msg, reply_markup=markup)


def stats(regim):
    reg = regim.split()
    if reg[0] == "FX_DAILY":
        time = 'дням'
    if reg[0] == "FX_WEEKLY":
        time = 'неделям'
    if reg[0] == "FX_MONTHLY":
        time = 'месяцам'
    query_array = reg[1].split('-')
    url = 'https://www.alphavantage.co/query?function={}&from_symbol={}&to_symbol={}&apikey={}'.format(
        reg[0], query_array[0], query_array[1], API_TOKEN)
    json_request = requests.get(url).json()
    col_name = 'Time Series FX ({})'.format(reg[0].split('_')[1].capitalize())

    try:
        data = pd.DataFrame.from_dict(json_request[col_name]).T
    except KeyError:
        return "Произошла ошибка: к сожалению, на сервере нет запрашиваемых данных."

    data = data.astype(float)
    means = round(data.mean(), 6)
    maxs = round(data.max(), 6)
    index_maxs = data.idxmax()
    mins = round(data.min(), 6)
    index_mins = data.idxmin()
    modas = round(data.mode(axis=0).iloc[0], 6)
    medians = np.around(np.median(data, axis=0), 6)
    quantils = np.around(np.quantile(data, q=[0.25, 0.75], axis=0), 6)
    disp = np.around(np.var(data, axis=0), 6)
    stds = np.around(np.std(data, axis=0), 6)
    stats_msg = 'Получены статистики пары {} по {} (с {} по {}), количество обработанных торговых дней - {}:\n' \
                '\n<b>Средние:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}\n' \
                '\n<b>Максимальное:</b>\n' \
                'open: {} &#8592; {}\n' \
                'high: {} &#8592; {}\n' \
                'low: {} &#8592; {}\n' \
                'close: {} &#8592; {}\n' \
                '\n<b>Минимальное:</b>\n' \
                'open: {} &#8592; {}\n' \
                'high: {} &#8592; {}\n' \
                'low: {} &#8592; {}\n' \
                'close: {} &#8592; {}\n' \
                '\n<b>Мода:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}\n' \
                '\n<b>Медиана:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}\n' \
                '\n<b>Квантили 0.25-го и 0.75-го порядка:</b>\n' \
                'open: {}, {}\n' \
                'high: {}, {}\n' \
                'low: {}, {}\n' \
                'close: {}, {}\n' \
                '\n<b>Дисперсия:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}\n' \
                '\n<b>Среднеквадратичное отклонение:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}\n' \
                '\n<b>Размах:</b>\n' \
                'open: {}\n' \
                'high: {}\n' \
                'low: {}\n' \
                'close: {}'.format(reg[1], time, data.index.min(), data.index.max(), len(data),
                                   means[0], means[1], means[2], means[3],
                                   maxs[0], index_maxs[0], maxs[1], index_maxs[1], maxs[2], index_maxs[2], maxs[3],
                                   index_maxs[3],
                                   mins[0], index_mins[0], mins[1], index_mins[1], mins[2], index_mins[2], mins[3],
                                   index_mins[3],
                                   modas[0], modas[1], modas[2], modas[3],
                                   medians[0], medians[1], medians[2], medians[3],
                                   quantils[0][0], quantils[1][0], quantils[0][1], quantils[1][1], quantils[0][2],
                                   quantils[1][2], quantils[0][3], quantils[1][3],
                                   disp[0], disp[0], disp[0], disp[0],
                                   stds[0], stds[1], stds[2], stds[3],
                                   round(maxs[0] - mins[0], 6), round(maxs[1] - mins[1], 6),
                                   round(maxs[2] - mins[2], 6), round(maxs[3] - mins[3], 6))

    return stats_msg


@bot.message_handler(
    content_types=["text", "audio", "document", "photo", "sticker", "video", "video_note", "voice", "location",
                   "contact"])
def check(message):
    if message.content_type == 'text':
        bot.send_message(message.chat.id, "Сначала выберите в /start необходимый анализ.")
    elif message.content_type == 'audio':
        bot.send_message(message.chat.id, "К сожалению я не умею слушать аудио.")
    elif message.content_type == 'document':
        bot.send_message(message.chat.id, "К сожалению я не умею считывать документы.")
    elif message.content_type == 'photo':
        bot.send_message(message.chat.id, "К сожалению я не вижу фотографии.")
    elif message.content_type == 'sticker':
        bot.send_message(message.chat.id, "К сожалению, я не вижу стикеры.")
    elif message.content_type == 'video' or message.content_type == 'video_note':
        bot.send_message(message.chat.id, "К сожалению, я не умею смотреть видео.")
    elif message.content_type == 'location':
        bot.send_message(message.chat.id, "Мне не нужна Ваша геолокация.")
    elif message.content_type == 'contact':
        bot.send_message(message.chat.id, "Мне не нужен Ваш номер.")


while True:
    try:
        bot.polling(none_stop=True)
    except Exception as e:
        print(f"Ошибка: {traceback.format_exc()}")