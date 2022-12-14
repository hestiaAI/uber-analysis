import fnmatch
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple
from zipfile import ZipFile
from typing import Callable, Any

import geopy.distance
import numpy as np
import swifter

import portion as P

from src.custom_types import *


def find_file(pattern: str, zf: ZipFile) -> str:
    """
    Looks for a file matching the given pattern inside the given folder
    :param pattern: a glob file pattern
    :param zf: a ZipFile object where the file should be searched
    :return: a path to the first file matching the pattern, or a value error if none.
    """
    matches = fnmatch.filter(zf.namelist(), pattern)
    if len(matches) == 0:
        raise ValueError(f'Could not find file {pattern} in {zf.filename}')
    elif len(matches) > 1:
        print(f'Found many matches for {pattern} in {zf.filename}. Using the first.')
    return matches[0]


def find_table(pattern: str, zf: ZipFile, usecols: Optional[list[str]] = None) -> Table['file': str]:
    """
    Looks inside folder for a csv-like file whose name matches the pattern, and only reads the specified columns.
    If found, reads it and assigns the file name as a column.
    :param pattern: a glob file pattern
    :param zf: a ZipFile object where the table should be found
    :param usecols: an optional list of columns present in the data file, only they will be loaded
    :return: a Table (a.k.a. pandas DataFrame) having the specified columns and the file name as a column.
    """
    filename = find_file(pattern, zf)
    print(f'Inner file {filename} open')
    with zf.open(filename, 'r') as f:
        table = pd.read_csv(BytesIO(f.read()), usecols=usecols)
    print(f'Inner file {filename} closed')
    if usecols is not None:
        table = table[usecols]
    return table.assign(file=filename)


def find_date_range(pattern: str, zf: ZipFile, date_cols: list[str]) -> (Timestamp, Timestamp):
    """

    :param pattern: a glob file pattern
    :param zf: a ZipFile object where the table should be found
    :param date_cols: a list of column names that are dates
    :return: the minimum and maximum observed dates
    Usage:
    find_date_range('*Driver Trip Status.csv', data_folder / 'Brice' / 'raw' / 'SAR',
                    ['begin_timestamp_local', 'end_timestamp_local'])
    """
    df = find_table(pattern, zf, usecols=date_cols)
    for c in date_cols:
        df[c] = pd.to_datetime(df[c])
    return df[date_cols].min().min(), df[date_cols].max().max()


def date_range(from_date: Tuple[int, ...], to_date: Tuple[int, ...]) -> list[Timestamp]:
    return list(pd.date_range(dt.date(*from_date), dt.date(*to_date)).values)


def scaled_interval(begin: Timestamp, end: Timestamp, attributes: dict, og_duration) -> dict:
    """Creates an interval (a dict with begin, end and other keys) given the original attributes and the original """
    return {'begin': begin, 'end': end,
            **{k: v * (end - begin) / og_duration if isinstance(v, float) else v for k, v in attributes.items()}}


def save_excel(filename: str | Path, sheets: dict[str, pd.DataFrame], float_format='%.2f', **kwargs):
    """Saves the given dictionary of dataframes as an Excel (xlsx) file."""
    """"""
    with pd.ExcelWriter(filename) as writer:
        for name, sheet in sheets.items():
            sheet.to_excel(writer, sheet_name=name, float_format=float_format, **kwargs)


def select(d: dict[str], keep: Optional[list[str]] = None, drop: Optional[list[str]] = None) -> dict[str]:
    assert (keep is None) != (drop is None), 'Only one of keep or drop can be specified'
    if keep is not None:
        return {k: d[k] for k in keep}
    if drop is not None:
        return {k: v for k, v in d.items() if k not in drop}


def find_week_limits(date: Timestamp) -> str:
    week_start = date - dt.timedelta(days=date.weekday())
    week_end = week_start + dt.timedelta(days=6)
    return f'{week_start.date()} to {week_end.date()}'.replace('-', '/').replace('to', '-')


def mile2km(n_miles: float) -> float:
    return n_miles * 1.609344


def df_to_interval(df: PeriodTable) -> P.interval:
    """Converts a DataFrame with columns 'begin' and 'end' into a Portion interval, merging entries that overlap."""
    return P.Interval(*[P.closed(row['begin'], row['end']) for row in df.to_dict('records')])


def interval_to_df(interval: P.interval) -> PeriodTable:
    """Converts a Portion interval into a dataframe with columns 'begin' and 'end'."""
    return pd.DataFrame([{'begin': begin, 'end': end} for _, begin, end, _ in P.to_data(interval)])


def make_status_intervals(df: PeriodTable) -> dict[str, P.interval]:
    return {s: df_to_interval(df[df.status == s]) for s in df.status.unique()}


def interval_merge_logic(lt: dict[str, P.interval], oo: dict[str, P.interval]) -> dict[str, P.interval]:
    P3 = lt['P3'] | oo['P3']
    P2 = (lt['P2'] | oo['P2']) - P3
    P1 = oo['P1'] - (P2 | P3)
    return {'P1': P1, 'P2': P2, 'P3': P3}


def main_interval_logic(lt: dict[str, P.interval], oo: dict[str, P.interval], P0_has_priority=False) -> Table:
    """Consider the following ordering of priorities: P3 > P2 > P1.
    P0_has_priority determines if P0 is on the left or right of inequalities."""
    if P0_has_priority:
        for d in [lt, oo]:
            for k in d.keys():
                if k != 'P0':
                    d[k] = d[k] - oo['P0']
    lt['P2'] = lt['P2'] - lt['P3']
    oo['P2'] = oo['P2'] - oo['P3']
    oo['P1'] = oo['P1'] - (oo['P2'] | oo['P3'])
    intervals = interval_merge_logic(lt, oo)
    return pd.concat([interval_to_df(i).assign(status=f'{s} consistent') for s, i in intervals.items()])


def time_tuples_to_periods(
        table: Table['t1': Timestamp, 't2': Timestamp, 't3': Timestamp],
        columns: list[str],
        extra_info: list[Callable[[pd.Series], dict]]
) -> pd.DataFrame:
    """
    Takes a dataframe where each row has N timestamps corresponding to instants of status changes,
    and converts each row into N-1 rows of periods in the corresponding status.

    :param: table: a table having a number N > 1 of time-columns and L of entries.
    :param: columns: a list of n time-column names present in {table}.
    :param: extra_info: a list of functions taking a row of df and outputting a dictionary of additional information. Cannot have keys 'begin' and 'end'.
    :return: periods: a table having L * (N-1) entries, each with a 'begin' and 'end' timestamp and associated information as specified by additional_info.
    Usage:
    df = pd.DataFrame([{'request_ts': '3:47 PM', 'begintrip_ts': '4:00 PM', 'dropoff_ts': '4:13 PM'}])
    columns = ['request_ts', 'begintrip_ts', 'dropoff_ts']
    extra_info = [lambda r: {'status': 'P2'}, lambda r: {'status': 'P3'}]
    time_tuples_to_periods(df, columns, extra_info)
    > begin    end      status
    > 3:47 PM  4:00 PM  P2
    > 4:00 PM  4:13 PM  P3
    """
    assert len(columns) == len(
        extra_info) + 1, f'The length of additional information should correspond to the number of generated periods (N-1).'
    periods = pd.DataFrame(table.swifter.apply(
        lambda r: [{'begin': r[b], 'end': r[e], **d(r)} for b, e, d in zip(columns, columns[1:], extra_info)],
        axis=1
    ).explode().to_list())
    return periods

def load_on_off(zf: ZipFile, timezone: str, pattern: str = '*Driver Online Offline.csv',
                birdeye_coefficient: float = 1.5) -> PeriodTable:
    table = find_table(pattern, zf,
                       ['begin_timestamp_utc', 'end_timestamp_utc', 'earner_state',
                        'begin_lat', 'begin_lng', 'end_lat', 'end_lng'])
    table.rename(columns={'begin_timestamp_utc': 'begin', 'end_timestamp_utc': 'end',
                          'earner_state': 'status'}, inplace=True)
    table = table.replace({r'\N': np.nan,
                           'ontrip': 'P3', 'on trip': 'P3',
                           'enroute': 'P2', 'en route': 'P2',
                           'open': 'P1', 'offline': 'P0'})
    gps_cols = ['begin_lat', 'begin_lng', 'end_lat', 'end_lng']
    for col in gps_cols:
        table[col] = table[col].astype(float)
    for col in ['begin', 'end']:
        table[col] = pd.to_datetime(table[col], utc=True).dt.tz_convert(timezone)
    table = table.dropna()
    table['birdeye_distance_km_x_1.5'] = table.swifter.apply(
        lambda r: birdeye_coefficient * geopy.distance.geodesic((r['begin_lat'], r['begin_lng']),
                                                                (r['end_lat'], r['end_lng'])).km, axis=1)
    # table.drop(columns=gps_cols, inplace=True)
    return table.dropna()


def load_lifetime_trips(zf: ZipFile, timezone: str, pattern: str = '*Driver Lifetime Trips.csv') -> PeriodTable:
    table = find_table(pattern, zf,
                       ['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc', 'status',
                        'request_to_begin_distance_miles', 'trip_distance_miles', 'original_fare_local'])
    table = table[table.status == 'completed'].drop(columns='status')
    table.replace({r'\N': np.nan}, inplace=True)
    for col in ['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc']:
        table[col] = pd.to_datetime(table[col], utc=True).dt.tz_convert(timezone)
    for col in ['request_to_begin_distance_miles', 'original_fare_local']:
        table[col] = table[col].astype(float)
    table = time_tuples_to_periods(
        table,
        columns=['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc'],
        extra_info=[
            lambda r: {'status': 'P2', 'distance_km': mile2km(r['request_to_begin_distance_miles']), 'file': r['file']},
            lambda r: {'status': 'P3', 'distance_km': mile2km(r['trip_distance_miles']), 'file': r['file'],
                       'uber_paid': r['original_fare_local']}])
    return table

french_months = {1: 'janvier', 2: 'f??vrier', 3: 'mars', 4: 'avril', 5: 'mai', 6: 'juin',
                 7: 'juillet', 8: 'ao??t', 9: 'septembre', 10: 'octobre', 11: 'novembre', 12: 'd??cembre'}

sar_text = """Bonjour,
Je m'adresse ?? vous pour vous demander de directement relayer mon message aupr??s du Responsable de la Protection des Donn??es d'Uber, Simon Hania, comme anticip?? par le R??glement G??n??ral sur la Protection des Donn??es europ??en. J'utilise pour ce faire un formulaire mis ?? disposition de ceux qui n'ont pas de compte Uber, mais qui me semble e??tre ma meilleure chance de contacter directement Simon Hania, ??tant donn??s donn??s les multiples probl??mes que vos services d'aide pr??sentent.
En effet, je conduis pour Uber et cherche ?? obtenir une copie de mes donn??es personnelles. La page https://help.uber.com/fr-CA/driving-and-delivering/article/demander-vos-donn%C3%A9es-personnelles-uber?nodeId=fbf08e68-65ba-456b-9bc6-1369eb9d2c44 m'informe que mes donn??es sont accessibles via le tableau de bord partenaire. Cependant, comme d??crit ??
https://forum.personaldata.io/t/transparence-sur-les-donnees-personnelles-chez-uber/307
la transparence offerte n'est pas suffisante ?? mon go??t. Votre page d'information m'invite ?? contacter le Responsable dans ce cas, ce que je fais maintenant.
Je cherche donc par la pr??sente ?? exercer mes droits pr??vus par le RGPD. Ceci inclut mon droit d'acc??s (Art 15), mon droit ?? la portabilit?? (Art 20). Je vous rappelle que tout violation des dispositions concernant les droits des personnes (Art 12 ?? 22) peuvent faire l'objet - en vertu de l'Article 83 - d'amendes administratives pouvant s'??lever jusqu'?? 20 000 000 EUR ou, dans le cas d'une entreprise, jusqu'?? 4 % du chiffre d'affaires annuel mondial total de l'exercice pr??c??dent, le montant le plus ??lev?? ??tant retenu.

Copie de mes donn??es personnelles
=================================
Cette requ??te couvre toutes mes donn??es personnelles, et en particulier celles concernant:
- ma g??olocalisation (y compris les empreintes temporelles associ??es, et les donn??es d'acc??l??rom??tre);
- mes revenus;
- les interactions partenaires;
- les interactions clients ?? mon propos, dont commentaires et notes;
- les ???exp??riences??? que Uber a mis en place et dont j'??tais sujet;
- les contrats, chartes et r??gles d'utilisation pour lesquels j'ai marqu?? mon accord, sous leurs diff??rentes versions, et dates associ??es;
- les notifications par email ou "push" qui m'ont ??t?? envoy??es, ainsi que mes interactions avec celles-ci;
- les offres qui m'ont ??t?? envoy??es, ainsi que mes interactions avec celles-ci;
- toute cote de performance, d??livr??e par Uber ou des clients, jointement ou s??par??ment;
- mon t??l??phone (y compris: niveau de batterie, syst??me d'exploitation, adresse IP, etc);
- mon v??hicule;
- mon accueil comme partneraire Uber;
- le dispatching et matching des courses pour lesquelles j'ai ??t?? retenu;
- les courses que j'ai effectu??es;
- les tickets internes Zendesk du service partenaires me concernant moi ou mes courses;
- les tickets internes Zendesk du service clients me concernant moi ou mes courses;
- les "tags" des services clients et partenaires me concernant moi ou mes courses;
- discussions internes ?? mon propos;
- toute d??connection, temporaire ou permanente;
- mes documents d'identit??, d'assurances, de validation, etc, y compris ce qui en a ??t?? extrait automatiquement;
- toute donn??e de profilage;
- la g??olocalisation, les empreintes temporelles et les donn??es d'acc??l??rom??tre du t??l??phone des clients lorsque nous nous trouvions simultan??ment dans mon v??hicule.

Concernant les donn??es de g??olocalisation, je voudrais obtenir l'enti??ret?? de ces donn??es d??tenues par Uber. N??anmoins mes confr??res m'informent que Uber restreint artificiellement sa r??ponse et n'inclut que le mois le plus r??cent, pour une prot??ger les "rights and freedoms of others". Uber sugg??re alors de pr??ciser des p??riodes additionnelles qu'elle ??valuera alors. Dans le but d'acc??l??rer ce processus, et tout en acceptant pas cette limitation artificielle de mes droits par Uber, je demande en particulier mes donn??es de g??olocalisation pour les p??riodes couvrant {REPLACE_HERE}.

Je vous informe de l'existence de Guidelines de l'European Data Protection Board sur le Right of Access (https://edpb.europa.eu/system/files/2022-01/edpb_guidelines_012022_right-of-access_0.pdf ). Ces guidelines incluent diff??rentes affirmations, telles que celles-ci: "Art. 15(4) GDPR should not result in a refusal to provide all information to the data subject.  This means, for example, where the limitation applies, that information concerning others has to be rendered illegible as far as possible instead of rejecting to provide a copy of the personal data." ou encore "he controller must be able to demonstrate that in the concrete situation rights or freedoms of others would factually be impacted." En cons??quence Uber se doit de justifier son refus de rendre toutes les donn??es de g??olocalisation, et de proposer des m??thodes alternatives.

Article 20
----------
Pour les donn??es tombant sous le coup du droit ?? la portabilit?? (RGPD Article 20), ce qui inclut toutes les donn??es fournies par moi (Article 29 Working Party, *Guidelines on the Right to Data Portability (WP 242)*, 13 D??cembre 2016, version fran??aise) et o?? la base juridique de traitement est le consentement ou le contrat, je souhaite:

- **recevoir ces donn??es dans un format structur??, couramment utilis?? et lisible par machine**
- accompagn?? avec un **description intelligible de toutes les variables**

Article 15
----------
Pour les donn??es tombant sous le coup du droit d'acc??s (RGPD Article 15), je voudrais **qu'une copie me soit envoy??e dans un format ??lectronique**. Veuillez noter que les donn??es qui vous sont disponibles dans un format lisible par une machine doivent m'??tre fournies sous cette forme aussi, au vu des principes de loyaut?? (RGPD Article 5.1.a) et de protection des donn??es d??s la conception.

Veuillez noter que toute opinion, d??duction, pr??f??rence etc sont consid??r??es comme des donn??es personnelles (voir le Cas C???434/16 *Peter Nowak v Data Protection Commissioner* [2017] ECLI:EU:C:2017:994, 34.)

J'ai bien conscience que certaines des donn??es que je demande concernent aussi des clients ou des employ??s de Uber. Je vous rappelle que dans ce cas il incombe ?? Uber de mettre en place des mesures pour prot??ger les droits de ces personnes, mais que cet argument ne peut ??tre utilis?? pour me refuser l'acc??s total ?? mes donn??es. Pour des donn??es de senseurs des appareils des clients lorsque ceux-ci se trouvent dans ma voiture par exemple, vous pourriez introduire un l??ger bruit dans les donn??es pour masquer toute caract??ristique individuelle de l'appareil.

Si vous me consid??rez comme responsable du traitement
-----------------------------------------------------
De plus, si vous me consid??rez comme responsable du traitement de certaines de ces donn??es et que vous agisseriez alors comme sous-traitant, veuillez me fournir alors toutes les donn??ess que vous traitez en mon nom dans un format lisible par une machine, en accord avec votre obligation de respecter ma volont?? sur les moyens et finalit??s du traitement.


M??tadonn??es sur le traitement
=============================

Cette requ??te concerne aussi les m??tadonn??es du traitement auxquelles j'ai droit suivant le RGPD.

Information sur les responsables du traitement, sous-traitants, sources et transferts
-------------------------------------------------------------------------------------
Je voudrais de plus conna??tre
- L'identit?? de tous les responsables conjoints du traitement, ainsi que grandes lignes de vos accords avec eux (RGPD Article 26).
- Tout **destinataire ?? qui vous avez r??v??l?? des donn??es**, nomm??es et avec leurs informations de contact en vertu de l'Article 15(1)(c). Veuillez noter que les r??gulateurs europ??ens ont affirm?? que par d??faut les responsables de traitement doivent nommer les destinataires et pas les "cat??gories" de destinataires. Ils affirment de plus: "Si les responsables du traitement choisissent de communiquer les cat??gories de destinataires, les informations devraient ??tre les plus sp??cifiques possible et indiquer le type de destinataire (en fonction des activit??s qu???il m??ne), l???industrie, le secteur et le sous-secteur ainsi que l???emplacement destinataires." (Article 29 Working Party, ???Guidelines on Transparency under Regulation 2016/679??? WP260 rev.01, 11 April 2018 ) Ils affirment de plus que la notion de "destinataire" est plus large que celle de "tiers", et inclut "les autres responsables du traitement, responsables conjoints du traitement et sous-traitants auxquels les donn??es sont transf??r??es ou communiqu??e". L'article 4 paragraphe 9 d??finit de plus les destinataires comme incluant toute autorit?? publique. Veuillez noter de plus que pour toute donn??e transf??r??e sur base du consentement, on ne peut nommer simplement la cat??gorie sans invalider la base juridique (Article 29 Working Party, ???Guidelines on Consent under Regulation 2016/679??? (WP259 rev.01, 10 April 2018) 13).
- Si tout donn??e n'a pas ??t?? collect??e, observ??e ou d??duite de moi directement, je voudrais de plus obtenir des informations pr??cises sur la **source de cette donn??e**, y compris le nom et l'adresse email de contact du responsable de traitement ("from which source the personal data originate", Article 14(2)(f)/15(1)(g)).
- Veuilez de plus confirmer o?? mes donn??es personnelles (y compris les backups) sont stock??es, et au moins si elles ont quitt?? l'Union Europ??enne (et si oui, veuillez me fournir les d??tails des bases l??gales et protections pour de tels transferts).

Information sur les finalit??s et les bases juridiques
-----------------------------------------------------
- Toutes les **finalit??s de traitement et les bases juridiques pour ces finalit??s par cat??gorie de donn??e personnelle** Cette liste doit ??tre fournie par finalit??, base juridique d??taill??e par finalit??, et cat??gorie de donn??es d??taill??e par finalit?? et base juridique. Des listes s??par??es sans alignement entre ces trois facteurs ne seraient pas acceptables (Article 29 Working Party, ???Guidelines on Transparency under Regulation 2016/679??? (WP260 rev.01, 11 April 2018), page 35.). Il se peut qu'une table soit la meilleure mani??re de fournir cette information.

- Les int??r??ts l??gitimes d??taill??s l?? o?? cette base juridique est utilis??e (Article 14(2)(b)).

Information sur les prises de d??cision automatis??es
---------------------------------------------------
- Veuillez confirmer si vous avez recours ou pas ?? des prises de d??cisions automatis??es (au sens de l'Article 22, RGPD). Si la r??ponse est oui, veuillez fournir des informations utiles sur la logique sous-jacente, ainsi que l'importance et les cons??quences pr??vues de ce traitement pour moi (Article 15(1)(h))

Information sur la conservation
-------------------------------
- Veuillez me confirmer pendant combien de temps chaque cat??gorie de donn??es est conserv??e, ainsi que les crit??res utilis??s pour prendre cette d??cision, en accord avec le principe de limitation de la conservation, et l'Article 15(1)(d).

Je me r??jouis de recevoir tr??s vite un accus?? de r??ception, et une r??ponse plus compl??te end??ans les 30 jours, comme anticip?? par le RGPD."""
