import fnmatch
from io import BytesIO
from pathlib import Path
from typing import Optional, Tuple
from zipfile import ZipFile

import numpy as np
import portion as P
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pyexcelerate import Workbook

from custom_types import *


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


def save_excel(filename: str | Path, sheets: dict[str, pd.DataFrame], float_format: str = '.2f'):
    """Saves the given dictionary of dataframes as an Excel (xlsx) file."""
    wb = Workbook()
    for name, sheet in sheets.items():
        for c in filter(lambda c: is_datetime(sheet[c]), sheet.columns):
            sheet[c] = sheet[c].dt.strftime('%Y-%m-%d %H:%M:%S')
        for c in sheet.select_dtypes(include=[np.float]).columns:
            sheet[c] = sheet[c].apply(lambda f: 0 if np.isnan(f) else round(f, 2))
        wb.new_sheet(name, data=[sheet.columns.tolist(), ] + sheet.values.tolist())
    wb.save(filename)


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


french_months = {1: 'janvier', 2: 'février', 3: 'mars', 4: 'avril', 5: 'mai', 6: 'juin',
                 7: 'juillet', 8: 'août', 9: 'septembre', 10: 'octobre', 11: 'novembre', 12: 'décembre'}

sar_text = """Bonjour,
Je m'adresse à vous pour vous demander de directement relayer mon message auprès du Responsable de la Protection des Données d'Uber, Simon Hania, comme anticipé par le Règlement Général sur la Protection des Données européen. J'utilise pour ce faire un formulaire mis à disposition de ceux qui n'ont pas de compte Uber, mais qui me semble être ma meilleure chance de contacter directement Simon Hania, étant donnés donnés les multiples problèmes que vos services d'aide présentent.
En effet, je conduis pour Uber et cherche à obtenir une copie de mes données personnelles. La page https://help.uber.com/fr-CA/driving-and-delivering/article/demander-vos-donn%C3%A9es-personnelles-uber?nodeId=fbf08e68-65ba-456b-9bc6-1369eb9d2c44 m'informe que mes données sont accessibles via le tableau de bord partenaire. Cependant, comme décrit à
https://forum.personaldata.io/t/transparence-sur-les-donnees-personnelles-chez-uber/307
la transparence offerte n'est pas suffisante à mon goût. Votre page d'information m'invite à contacter le Responsable dans ce cas, ce que je fais maintenant.
Je cherche donc par la présente à exercer mes droits prévus par le RGPD. Ceci inclut mon droit d'accès (Art 15), mon droit à la portabilité (Art 20). Je vous rappelle que tout violation des dispositions concernant les droits des personnes (Art 12 à 22) peuvent faire l'objet - en vertu de l'Article 83 - d'amendes administratives pouvant s'élever jusqu'à 20 000 000 EUR ou, dans le cas d'une entreprise, jusqu'à 4 % du chiffre d'affaires annuel mondial total de l'exercice précédent, le montant le plus élevé étant retenu.

Copie de mes données personnelles
=================================
Cette requête couvre toutes mes données personnelles, et en particulier celles concernant:
- ma géolocalisation (y compris les empreintes temporelles associées, et les données d'accéléromètre);
- mes revenus;
- les interactions partenaires;
- les interactions clients à mon propos, dont commentaires et notes;
- les “expériences” que Uber a mis en place et dont j'étais sujet;
- les contrats, chartes et règles d'utilisation pour lesquels j'ai marqué mon accord, sous leurs différentes versions, et dates associées;
- les notifications par email ou "push" qui m'ont été envoyées, ainsi que mes interactions avec celles-ci;
- les offres qui m'ont été envoyées, ainsi que mes interactions avec celles-ci;
- toute cote de performance, délivrée par Uber ou des clients, jointement ou séparément;
- mon téléphone (y compris: niveau de batterie, système d'exploitation, adresse IP, etc);
- mon véhicule;
- mon accueil comme partneraire Uber;
- le dispatching et matching des courses pour lesquelles j'ai été retenu;
- les courses que j'ai effectuées;
- les tickets internes Zendesk du service partenaires me concernant moi ou mes courses;
- les tickets internes Zendesk du service clients me concernant moi ou mes courses;
- les "tags" des services clients et partenaires me concernant moi ou mes courses;
- discussions internes à mon propos;
- toute déconnection, temporaire ou permanente;
- mes documents d'identité, d'assurances, de validation, etc, y compris ce qui en a été extrait automatiquement;
- toute donnée de profilage;
- la géolocalisation, les empreintes temporelles et les données d'accéléromètre du téléphone des clients lorsque nous nous trouvions simultanément dans mon véhicule.

Concernant les données de géolocalisation, je voudrais obtenir l'entièreté de ces données détenues par Uber. Néanmoins mes confrères m'informent que Uber restreint artificiellement sa réponse et n'inclut que le mois le plus récent, pour une protéger les "rights and freedoms of others". Uber suggère alors de préciser des périodes additionnelles qu'elle évaluera alors. Dans le but d'accélérer ce processus, et tout en acceptant pas cette limitation artificielle de mes droits par Uber, je demande en particulier mes données de géolocalisation pour les périodes couvrant {REPLACE_HERE}.

Je vous informe de l'existence de Guidelines de l'European Data Protection Board sur le Right of Access (https://edpb.europa.eu/system/files/2022-01/edpb_guidelines_012022_right-of-access_0.pdf ). Ces guidelines incluent différentes affirmations, telles que celles-ci: "Art. 15(4) GDPR should not result in a refusal to provide all information to the data subject.  This means, for example, where the limitation applies, that information concerning others has to be rendered illegible as far as possible instead of rejecting to provide a copy of the personal data." ou encore "he controller must be able to demonstrate that in the concrete situation rights or freedoms of others would factually be impacted." En conséquence Uber se doit de justifier son refus de rendre toutes les données de géolocalisation, et de proposer des méthodes alternatives.

Article 20
----------
Pour les données tombant sous le coup du droit à la portabilité (RGPD Article 20), ce qui inclut toutes les données fournies par moi (Article 29 Working Party, *Guidelines on the Right to Data Portability (WP 242)*, 13 Décembre 2016, version française) et où la base juridique de traitement est le consentement ou le contrat, je souhaite:

- **recevoir ces données dans un format structuré, couramment utilisé et lisible par machine**
- accompagné avec un **description intelligible de toutes les variables**

Article 15
----------
Pour les données tombant sous le coup du droit d'accès (RGPD Article 15), je voudrais **qu'une copie me soit envoyée dans un format électronique**. Veuillez noter que les données qui vous sont disponibles dans un format lisible par une machine doivent m'être fournies sous cette forme aussi, au vu des principes de loyauté (RGPD Article 5.1.a) et de protection des données dès la conception.

Veuillez noter que toute opinion, déduction, préférence etc sont considérées comme des données personnelles (voir le Cas C‑434/16 *Peter Nowak v Data Protection Commissioner* [2017] ECLI:EU:C:2017:994, 34.)

J'ai bien conscience que certaines des données que je demande concernent aussi des clients ou des employés de Uber. Je vous rappelle que dans ce cas il incombe à Uber de mettre en place des mesures pour protéger les droits de ces personnes, mais que cet argument ne peut être utilisé pour me refuser l'accès total à mes données. Pour des données de senseurs des appareils des clients lorsque ceux-ci se trouvent dans ma voiture par exemple, vous pourriez introduire un léger bruit dans les données pour masquer toute caractéristique individuelle de l'appareil.

Si vous me considérez comme responsable du traitement
-----------------------------------------------------
De plus, si vous me considérez comme responsable du traitement de certaines de ces données et que vous agisseriez alors comme sous-traitant, veuillez me fournir alors toutes les donnéess que vous traitez en mon nom dans un format lisible par une machine, en accord avec votre obligation de respecter ma volonté sur les moyens et finalités du traitement.


Métadonnées sur le traitement
=============================

Cette requête concerne aussi les métadonnées du traitement auxquelles j'ai droit suivant le RGPD.

Information sur les responsables du traitement, sous-traitants, sources et transferts
-------------------------------------------------------------------------------------
Je voudrais de plus connaître
- L'identité de tous les responsables conjoints du traitement, ainsi que grandes lignes de vos accords avec eux (RGPD Article 26).
- Tout **destinataire à qui vous avez révélé des données**, nommées et avec leurs informations de contact en vertu de l'Article 15(1)(c). Veuillez noter que les régulateurs européens ont affirmé que par défaut les responsables de traitement doivent nommer les destinataires et pas les "catégories" de destinataires. Ils affirment de plus: "Si les responsables du traitement choisissent de communiquer les catégories de destinataires, les informations devraient être les plus spécifiques possible et indiquer le type de destinataire (en fonction des activités qu’il mène), l’industrie, le secteur et le sous-secteur ainsi que l’emplacement destinataires." (Article 29 Working Party, ‘Guidelines on Transparency under Regulation 2016/679’ WP260 rev.01, 11 April 2018 ) Ils affirment de plus que la notion de "destinataire" est plus large que celle de "tiers", et inclut "les autres responsables du traitement, responsables conjoints du traitement et sous-traitants auxquels les données sont transférées ou communiquée". L'article 4 paragraphe 9 définit de plus les destinataires comme incluant toute autorité publique. Veuillez noter de plus que pour toute donnée transférée sur base du consentement, on ne peut nommer simplement la catégorie sans invalider la base juridique (Article 29 Working Party, ‘Guidelines on Consent under Regulation 2016/679’ (WP259 rev.01, 10 April 2018) 13).
- Si tout donnée n'a pas été collectée, observée ou déduite de moi directement, je voudrais de plus obtenir des informations précises sur la **source de cette donnée**, y compris le nom et l'adresse email de contact du responsable de traitement ("from which source the personal data originate", Article 14(2)(f)/15(1)(g)).
- Veuilez de plus confirmer où mes données personnelles (y compris les backups) sont stockées, et au moins si elles ont quitté l'Union Européenne (et si oui, veuillez me fournir les détails des bases légales et protections pour de tels transferts).

Information sur les finalités et les bases juridiques
-----------------------------------------------------
- Toutes les **finalités de traitement et les bases juridiques pour ces finalités par catégorie de donnée personnelle** Cette liste doit être fournie par finalité, base juridique détaillée par finalité, et catégorie de données détaillée par finalité et base juridique. Des listes séparées sans alignement entre ces trois facteurs ne seraient pas acceptables (Article 29 Working Party, ‘Guidelines on Transparency under Regulation 2016/679’ (WP260 rev.01, 11 April 2018), page 35.). Il se peut qu'une table soit la meilleure manière de fournir cette information.

- Les intérêts légitimes détaillés là où cette base juridique est utilisée (Article 14(2)(b)).

Information sur les prises de décision automatisées
---------------------------------------------------
- Veuillez confirmer si vous avez recours ou pas à des prises de décisions automatisées (au sens de l'Article 22, RGPD). Si la réponse est oui, veuillez fournir des informations utiles sur la logique sous-jacente, ainsi que l'importance et les conséquences prévues de ce traitement pour moi (Article 15(1)(h))

Information sur la conservation
-------------------------------
- Veuillez me confirmer pendant combien de temps chaque catégorie de données est conservée, ainsi que les critères utilisés pour prendre cette décision, en accord avec le principe de limitation de la conservation, et l'Article 15(1)(d).

Je me réjouis de recevoir très vite un accusé de réception, et une réponse plus complète endéans les 30 jours, comme anticipé par le RGPD."""
