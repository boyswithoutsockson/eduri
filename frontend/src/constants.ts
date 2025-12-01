/** Used by search inputs for sending the search terms to the backend */
export const SEARCH_QUERYPARAM = "q";

/** Used by paginations for sending the current page to the backend */
export const PAGE_QUERYPARAM = "p";

/** Incomplete mapping from database `parties` table ids to colors from
 * https://fi.wikipedia.org/wiki/Luokka:Suomen_puolueiden_v%C3%A4rimallineet */
export const PARTY_COLORS = {
    "demokraattinen vaihtoehto": "#8B0000",
    "eduskuntaryhmä valta kuuluu kansalle": "#214627",
    "isänmaallinen kansanliike": "#000008",
    "kansallinen edistyspuolue": "#ffd700",
    kansanpuolue: "#47C7E6",
    kd: "#2B67C9",
    kesk: "#01954B",
    kok: "#006288",
    "kristillisen liiton eduskuntaryhmä": "#18359B",
    "liberaalisen kansanpuolueen eduskuntaryhmä": "#F1AF44",
    liik: "#ae2375",
    "muutos 2011 eduskuntaryhmä": "#004460",
    "nuorsuomalainen puolue": "#3399FF",
    "nuorsuomalaisten eduskuntaryhmä": "#109473",
    ps: "#FFD500",
    r: "#FFDD93",
    remonttiryhmä: "#FFD600",
    sd: "#E11931",
    "sininen eduskuntaryhmä": "#031F73",
    "suomalainen puolue": "#3333FF",
    "suomen kansan demokraattisen liiton eduskuntaryhmä": "#BF1E24",
    "suomen kansanpuolue": "#3377ff",
    "suomen kristillisen työväen liitto": "#9400d3",
    "suomen maaseudun puolueen eduskuntaryhmä": "#053170",
    "työväen ja pienviljelijäin sosialidemokraattinen liitto": "#DA2300",
    "tähtiliikkeen eduskuntaryhmä": "#239dac",
    vas: "#BF1E24",
    vihr: "#61BF1A",
};

export const PARTY_ABBRS = {
    kd: "KD",
    kesk: "Kesk.",
    kok: "Kok.",
    liik: "Liik.",
    ps: "PS",
    r: "RKP",
    sd: "SDP",
    vas: "Vas.",
    vihr: "Vihr.",
};

export const SPEECH_TYPE_STYLES = {
    E: { style: "E", text: "Esittelypuheenvuoro" },
    V: { style: "V", text: "Vastauspuheenvuoro" },
    N: { style: "N", text: "Nopeatahtinen keskustelu" },
    R: { style: "R", text: "Ryhmäpuheenvuoro" },
    T: { style: "T", text: "" }, // varsinainen puheenvuoro, the default
    " ": { style: " ", text: "" }, // Faults in the data
};
