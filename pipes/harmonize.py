def harmonize_parliamentary_group(parliamentary_group):
    if parliamentary_group in [
        "vas",
        "Vasemmistoliiton eduskuntaryhmä",
        "Left Alliance Parliamentary Group",
    ]:
        parliamentary_group = "vas"

    elif parliamentary_group in [
        "kok",
        "Kansallisen kokoomuksen eduskuntaryhmä",
        "Parliamentary Group of the National Coalition parliamentary_group",
    ]:
        parliamentary_group = "kok"

    elif parliamentary_group in [
        "ps",
        "Perussuomalaisten eduskuntaryhmä",
        "The Finns parliamentary_group Parliamentary Group",
    ]:
        parliamentary_group = "ps"

    elif parliamentary_group in [
        "vihr",
        "Vihreä eduskuntaryhmä",
        "Green Parliamentary Group",
    ]:
        parliamentary_group = "vihr"

    elif parliamentary_group in [
        "sd",
        "Sosialidemokraattinen eduskuntaryhmä",
        "Social Democratic Parliamentary Group",
    ]:
        parliamentary_group = "sd"

    elif parliamentary_group in [
        "kd",
        "Kristillisdemokraattinen eduskuntaryhmä",
        "Christian Democratic Parliamentary Group",
    ]:
        parliamentary_group = "kd"

    elif parliamentary_group in [
        "kesk",
        "Keskustan eduskuntaryhmä",
        "Centre parliamentary_group Parliamentary Group",
    ]:
        parliamentary_group = "kesk"

    elif parliamentary_group in [
        "r",
        "Ruotsalainen eduskuntaryhmä",
        "Swedish Parliamentary Group",
    ]:
        parliamentary_group = "r"

    elif parliamentary_group in [
        "liik",
        "Liike Nyt -eduskuntaryhmä",
        "Liike Nyt-Movement's Parliamentary Group",
    ]:
        parliamentary_group = "liik"

    elif parliamentary_group == "Eduskuntaryhmään kuulumaton":
        parliamentary_group = "-"

    else:
        parliamentary_group = parliamentary_group.lower()

    return parliamentary_group
