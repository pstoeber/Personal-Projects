import pymysql
import logging
import datetime

def update_names(connection):
    table_list = ['basic_box_stats', 'advanced_box_stats', 'player_usage_stats', 'player_misc_stats', 'player_scoring_stats']
    update_name_dict = {'James Webb':'James Webb III',
                        'James Ennis':'James Ennis III',
                        'Wade Baldwin':'Wade Baldwin IV',
                        'Danuel House':'Danuel House Jr.',
                        'Maxi Kleber':'Maximilian Kleber',
                        'Taurean Waller-Prince':'Taurean Prince',
                        'Derrick Jones':'Derrick Jones Jr.',
                        'Dennis Smith':'Dennis Smith Jr.',
                        'Frank Mason':'Frank Mason III',
                        'Andrew White':'Andrew White III',
                        'Derrick Walton':'Derrick Walton Jr.',
                        'Larry Drew':'Larry Drew II',
                        'Walt Lemon, Jr.':'Walt Lemon Jr.',
                        'Johnny O\'Bryant':'Johnny O\'Bryant III',
                        'Matt Williams':'Matt Williams Jr.',
                        'A.J. Hammons':'AJ Hammons',
                        'K.J. McDaniels':'KJ McDaniels',
                        'Erik Murphy':'Erik Jay Murphy',
                        'Viacheslav Kravtsov':'Slava Kravtsov',
                        'Hamady N\'Diaye':'Mamadou N\'diaye',
                        'Dwayne Jones':'Dwayne Jones II',
                        'Roko Ukic':'Roko-Leni Ukic',
                        'Walter Herrmann':'Walter Herrmann Heinrich',
                        'Viktor Khryapa':'Victor Khryapa',
                        'Mike Sweetney':'Michael Sweetney',
                        'Stanislav Medvedenko':'Slava Medvedenko',
                        'Ha Seung-Jin':'Seung Jin Ha',
                        'Wang Zhizhi':'ZhiZhi Wang',
                        'Ibo Kutluay':'Ibrahim Kutluay',
                        'Ruben Boumtje-Boumtje':'Ruben Boumtje Boumtje',
                        'Efthimi Rentzias':'Efthimios Rentzias',
                        'Joe Crispin':'Joseph Crispin',
                        'William Avery':'Will Avery',
                        'Isaac Fontaine':'Ike Fontaine',
                        'Cedric Henderson':'Cedric E. Henderson',
                        'Jeffrey Sheppard':'Jeff Sheppard',
                        'J.R. Smith':'JR Smith',
                        'Kelly Oubre.':'Kelley Oubre Jr.',
                        'Roger Mason':'Roger Mason Jr.',
                        'Patrick Ewing':'Patrick Ewing Jr.',
                        'Glen Rice':'Glen Rice Jr.',
                        'Tim Hardaway':'Tim Hardaway Jr.',
                        'Larry Nance':'Larry Nance Jr.',
                        'Otto Porter':'Otto Porter Jr.',
                        'P.J. Tucker':'PJ Tucker',
                        'T.J. Warren':'TJ Warren',
                        'J.J. Redick':'JJ Redick',
                        'C.J. Williams':'CJ Williams',
                        'Marvin Bagley':'Marvin Bagley III',
                        'Jaren Jackson':'Jaren Jackson Jr.',
                        'Wendell Carter':'Wendell Carter Jr.',
                        'Cedric Henderson':'Cedric E. Henderson',
                        'Flip Murray':'Ronald Murray',
                        'Clar. Weatherspoon':'Clarence Weatherspoon',
                        'Dan Schayes':'Danny Schayes',
                        'Wang Zhi-zhi':'ZhiZhi Wang',
                        'Nene':'Nene Hilario',
                        'Charles R. Jones':'Charles Jones',
                        'Ike Austin':'Isaac Austin',
                        'Richard Manning':'Rich Manning',
                        'William Avery':'Will Avery',
                        'Hot Rod Williams':'Elhassan Williams',
                        'Norman Richardson':'Norm Richardson',
                        'Joe Crispin':'Joseph Crispin',
                        'Ruben Boumtje-Boumtje':'Ruben Boumtje Boumtje',
                        'Viktor Khryapa':'Victor Khryapa',
                        'Michael McDonald':'Mike McDonald',
                        'DJ Mbenga':'Didier Ilunga-Mbenga',
                        'Boniface Ndong':'Boniface N\'Dong',
                        'Dwayne Jones':'Dwayne Jones II',
                        'Walter Herrmann':'Walter Herrmann Heinrich',
                        'DJ Strawberry':'D.J. Strawberry',
                        'DJ White':'D.J. White',
                        'Roko Ukic':'Roko-Leni Ukic',
                        'AJ Price':'A.J. Price',
                        'Sun Sun':'Sun Yue',
                        'Perry Jones III':'Perry Jones',
                        'Viacheslav Kravtsov':'Slava Kravtsov',
                        'Gigi Datome':'Luigi Datome',
                        'Erik Murphy':'Erik Jay Murphy',
                        'PJ Hairston':'P.J. Hairston',
                        'RJ Hunter':'R.J. Hunter',
                        'Juancho Hernangomez':'Juan Hernangomez',
                        'Wes Iwundu':'Wesley Iwundu',
                        'TJ Leaf':'T.J. Leaf',
                        'Maxi Kleber':'Maximilian Kleber',
                        'Frank Mason':'Frank Mason III',
                        'Svi Mykhailiuk':'Sviatoslav Mykhailiuk',
                        'Harry Giles III':'Harry Giles',
                        'Robert Williams III':'Robert Williams',
                        'Walter Lemon Jr.':'Walt Lemon Jr.',
                        'JJ O\'Brien':'J.J. O\'Brien',
                        'Vincent Hunter':'Vince Hunter',
                        'C.J. Miles':'CJ Miles',
                        'JJ Hickson':'J.J. Hickson',
                        'Kelly Oubre Jr.':'Kelley Oubre Jr.',
                        'CJ Wilcox':'C.J. Wilcox'}

    for k, v in update_name_dict.items():
        for c, table in enumerate(table_list):
            if c < 2:
                field = 'name'
            else:
                field = 'player'
            update = 'update {} set {} = "{}" where {} = "{}"'.format(table, field, v, field, k)
            sql_execute(connection, update)

def sql_execute(connection, update):
    exe = connection.cursor()
    exe.execute(update)

def main():
    logging.basicConfig(filename='nba_stat_incrementals_log.log', filemode='a', level=logging.INFO)
    logging.info('Beginning NBA Reference player name update {}'.format(str(datetime.datetime.now())))
    connection = pymysql.connect(host='localhost', user='root', password='Sk1ttles', db='nba_stats_staging', autocommit=True)
    update_names(connection)
    logging.info('NBA Reference player name update completed successfully {}'.format(str(datetime.datetime.now())))

if __name__ == '__main__':
    main()
