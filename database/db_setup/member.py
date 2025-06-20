import sqlalchemy as db
from datetime import datetime
import pandas as pd


def setup(engine):
    metadata = db.MetaData()

    member = db.Table(
        'member',
        metadata,
        db.Column('member_id', db.Integer, primary_key=True, nullable=False),
        db.Column('first_name', db.String(50), nullable=False),
        db.Column('last_name', db.String(50), nullable=False),
        db.Column('start_date', db.String(50), nullable=False)
    )
    metadata.create_all(engine)


def populate(engine, member):
    metadata = db.MetaData()
    member = db.Table('member', metadata, autoload_with=engine)

    records = [
        (1992,'Jacob','SMITH','2018-06-19'),
        (1923,'Jayden','JOHNSON','2018-06-26'),
        (1134,'Daniel','WILLIAMS','2018-10-22'),
        (1589,'Ethan','BROWN','2018-07-19'),
        (1346,'Matthew','JONES','2018-03-26'),
        (1511,'Noah','GARCIA','2018-04-22'),
        (1972,'Anthony','MILLER','2018-06-25'),
        (1413,'Alexander','DAVIS','2018-06-11'),
        (1272,'Nathan','RODRIGUEZ','2018-11-25'),
        (1942,'David','MARTINEZ','2018-03-14'),
        (1487,'Andrew','HERNANDEZ','2018-02-10'),
        (1251,'Aiden','LOPEZ','2018-08-13'),
        (1292,'Michael','GONZALEZ','2018-12-24'),
        (1061,'Angel','WILSON','2018-08-28'),
        (1013,'Isaac','ANDERSON','2018-12-24'),
        (1207,'Julian','THOMAS','2018-10-19'),
        (1385,'Mason','TAYLOR','2018-02-16'),
        (1371,'Adrian','MOORE','2018-07-17'),
        (1289,'Jonathan','JACKSON','2018-09-12'),
        (1856,'Christopher','MARTIN','2018-02-11'),
        (1200,'Joshua','LEE','2018-05-12'),
        (1277,'Benjamin','PEREZ','2018-09-15'),
        (1636,'Joseph','THOMPSON','2018-03-16'),
        (1795,'Liam','WHITE','2018-10-22'),
        (1549,'Jose','HARRIS','2018-12-21'),
        (1307,'Dylan','SANCHEZ','2018-09-15'),
        (1520,'Aaron','CLARK','2018-01-21'),
        (1886,'Elijah','RAMIREZ','2018-11-18'),
        (1865,'Ryan','LEWIS','2018-04-26'),
        (1454,'Sebastian','ROBINSON','2018-10-15'),
        (1540,'William','WALKER','2018-12-19'),
        (1721,'Logan','YOUNG','2018-05-17'),
        (1335,'Christian','ALLEN','2018-12-20'),
        (1298,'Gabriel','KING','2018-02-16'),
        (1368,'Brandon','WRIGHT','2018-10-10'),
        (1650,'Samuel','SCOTT','2018-04-25'),
        (1660,'Damian','TORRES','2018-05-26'),
        (1925,'James','NGUYEN','2018-12-22'),
        (1997,'Isaiah','HILL','2018-03-19'),
        (1460,'Kevin','FLORES','2018-07-10'),
        (1422,'Lucas','GREEN','2018-09-21'),
        (1765,'Sophia','ADAMS','2018-09-13'),
        (1232,'Isabella','NELSON','2018-10-21'),
        (1928,'Emma','BAKER','2018-01-12'),
        (1074,'Emily','HALL','2018-01-22'),
        (1475,'Mia','RIVERA','2018-02-20'),
        (1010,'Olivia','CAMPBELL','2018-05-24'),
        (1164,'Sofia','MITCHELL','2018-08-12'),
        (1884,'Abigail','CARTER','2018-09-18'),
        (1100,'Samantha','ROBERTS','2018-05-20'),
        (1333,'Camila','PHILLIPS','2018-05-10'),
        (1331,'Ava','EVANS','2018-10-24'),
        (1194,'Victoria','TURNER','2018-09-15'),
        (1336,'Natalie','DIAZ','2018-10-22'),
        (1366,'Chloe','PARKER','2018-03-24'),
        (1792,'Elizabeth','CRUZ','2018-09-10'),
        (1620,'Evelyn','EDWARDS','2018-10-16'),
        (1506,'Genesis','COLLINS','2018-03-19'),
        (1310,'Ashley','REYES','2018-01-18'),
        (1142,'Madison','STEWART','2018-04-24'),
        (1798,'Zoe','MORRIS','2018-12-26'),
        (1372,'Charlotte','MORALES','2018-04-28'),
        (1250,'Hailey','MURPHY','2018-08-23'),
        (1509,'Melanie','COOK','2018-05-13'),
        (1294,'Audrey','ROGERS','2018-02-16'),
        (1101,'Kimberly','GUTIERREZ','2018-01-28'),
        (1406,'Alexa','ORTIZ','2018-12-19'),
        (1881,'Alyssa','MORGAN','2018-12-16'),
        (1706,'Allison','COOPER','2018-03-28'),
        (1361,'Aubrey','PETERSON','2018-11-21'),
        (1234,'Zoey','BAILEY','2018-03-11'),
        (1367,'Ella','REED','2018-01-21'),
        (1900,'Grace','KELLY','2018-02-26'),
        (1568,'Lily','HOWARD','2018-10-14'),
        (1414,'Bella','RAMOS','2018-05-24'),
        (1031,'Kaylee','KIM','2018-06-27'),
        (1452,'Leah','COX','2018-04-24'),
        (1489,'Andrea','WARD','2018-05-25'),
        (1595,'Aaliyah','RICHARDSON','2018-07-24'),
        (1222,'Brianna','WATSON','2018-01-18'),
        (1632,'Hannah','BROOKS','2018-05-21'),
        (1402,'Scarlett','CHAVEZ','2018-09-15'),
        (1513,'Jacob','WOOD','2018-04-18'),
        (1675,'Jayden','JAMES','2018-05-26'),
        (1085,'Daniel','BENNETT','2018-02-15'),
        (1282,'Ethan','GRAY','2018-08-19'),
        (1570,'Matthew','MENDOZA','2018-03-11'),
        (1684,'Noah','RUIZ','2018-07-16'),
        (1030,'Anthony','HUGHES','2018-04-14'),
        (1898,'Alexander','PRICE','2018-12-22'),
        (1967,'Nathan','ALVAREZ','2018-12-24'),
        (1761,'David','CASTILLO','2018-10-26'),
        (1522,'Andrew','SANDERS','2018-05-20'),
        (1165,'Aiden','PATEL','2018-10-10'),
        (1973,'Michael','MYERS','2018-12-15'),
        (1087,'Angel','LONG','2018-10-22'),
        (1701,'Isaac','ROSS','2018-12-18'),
        (1697,'Julian','FOSTER','2018-03-14'),
        (1855,'Mason','JIMENEZ','2018-02-24'),
        (1580,'Adrian','POWELL','2018-09-22'),
        (1625,'Jonathan','JENKINS','2018-07-14')
    ]

    data_to_insert = [
        {
            "member_id": r[0],
            "first_name": r[1],
            "last_name": r[2],
            "start_date": datetime.strptime(r[3], "%Y-%m-%d").date()
        }
        for r in records
    ]

    with engine.connect() as conn:
        conn.execute(member.insert(), data_to_insert)
        conn.commit()


def remove(engine):
    metadata = db.MetaData()
    member = db.Table('member', metadata, autoload_with=engine)
    member.drop(engine)
