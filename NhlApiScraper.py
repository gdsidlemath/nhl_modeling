from __future__ import division
import requests
from datetime import datetime
import pandas as pd
import json
import sqlite3 as lite
import sys

class MlbApiScraper:

    def __init__(self,
                 seasons=None,
                 months=None,
                 days=None,
                 teams=None,
                 as_db=False,
                 as_csv=False,
                 db_name="",
                 ds_name=""):

        if seasons is None:
            seasons = list(range(2010, 2020))
        elif not isinstance(seasons, (tuple, list)):
            seasons = [seasons]
        else:
            seasons = list(range(seasons[0], seasons[1] + 1))
        if months is None:
            months = [3, 11]
        elif not isinstance(months, (tuple, list)):
            months = [months]
        if days is None:
            days = [1, 30]
        elif not isinstance(days, (tuple, list)):
            days = [days]

        # *** only add "flo" distinction if interest is in specific teams ***
        if teams is not None:
            if min(seasons) < 2012:
                if any(t == "mia" for t in teams) and not any(t == "flo" for t in teams):
                    teams.append("flo")
                if any(t == "flo" for t in teams) and not any(t == "mia" for t in teams):
                    teams.append("mia")

        if teams is None:
            teams = ["ana", "nya", "bal", "cle", "chn", "was", "det", "cha", "hou",
                     "tor", "mia", "col", "mil", "min", "nyn", "ari", "oak", "bos",
                     "pit", "atl", "sdn", "cin", "sfn", "phi", "sln", "lan",
                     "tba", "sea", "tex", "kca"]

        self.opening_day_dict = {2009: (4, 5),
                                 2010: (4, 4),
                                 2011: (3, 31),
                                 2012: (3, 28),
                                 2013: (3, 31),
                                 2014: (3, 30),
                                 2015: (4, 5),
                                 2016: (4, 3),
                                 2017: (4, 2),
                                 2018: (3, 29),
                                 2019: (3, 28),
                                 2020: (3, 26)}

        self.seasons = seasons
        self.months = months
        self.days = days
        self.teams = teams

        self.as_db = as_db
        self.as_csv = as_csv
        self.ds_name = ds_name
        self.db_name = db_name

    def get_raw_url_data(self,
                         url):

        page = requests.get(url)

        return page.content

    def get_api_id_data(self):

        if len(self.days) == 1 and len(self.months) == 1 and len(self.seasons) == 1:
            start_date = str(self.months[0]).zfill(2) + "/" + str(self.days[0]).zfill(2) + "/" + str(self.seasons)
            end_date = start_date
        else:
            if len(self.seasons) > 1:
                date_list = []
                for season in self.seasons:
                    start_date = str(min(self.months)).zfill(2) + "/" + str(min(self.days)).zfill(2) + "/" + str(season)
                    end_date = str(max(self.months)).zfill(2) + "/" + str(max(self.days)).zfill(2) + "/" + str(season)
                    date_list.append((start_date, end_date))
            else:
                start_date = str(min(self.months)).zfill(2) + "/" + str(min(self.days)).zfill(2) + "/" + str(self.seasons)
                end_date = str(max(self.months)).zfill(2) + "/" + str(max(self.days)).zfill(2) + "/" + str(self.seasons)

        if len(self.seasons) > 1:
            json_list = []
            for sd, ed in date_list:
                url = "https://statsapi.nhl.com/api/v1/schedule/?startDate=" + sd + "&endDate=" + ed

                raw = self.get_raw_url_data(url)

                raw_json = json.loads(raw)

                json_list.append(raw_json)
        else:
            url = "https://statsapi.mlb.com/api/v1/schedule/?startDate=" + start_date + "&endDate=" + end_date

            raw = self.get_raw_url_data(url)

            raw_json = json.loads(raw)

            json_list = [raw_json]

        return json_list

    def get_api_game_data(self, gid):

        url = "https://statsapi.nhl.com/api/v1/game/" + str(gid) + "/feed/live"

        raw = self.get_raw_url_data(url)

        raw_json = json.loads(raw)

        return raw_json

    def get_all_api_game_dfs(self):

        base_list = self.get_api_id_data()

        date_list = [b["dates"] for b in base_list]

        day_dicts = [sd["games"] for d in date_list for sd in d]

        temp_id_list = []

        for d_dict in day_dicts:
            day_gid_list = [b["gamePk"] for b in d_dict if b["gameType"] in ["R", "P"]]
            temp_id_list.append(day_gid_list)

        all_id_list = [gid for gid_list in temp_id_list for gid in gid_list]

        all_game_list, all_ab_list, all_pitch_list = [], [], []
        for gid in all_id_list:
            raw_data = self.get_api_game_data(gid=gid)
            df_list = self.build_game_dataframes(raw_data)
            if not any([df is None for df in df_list]):
                game_df, ab_df, pitch_df = df_list
                all_game_list.append(game_df.reset_index(drop=True))
                all_ab_list.append(ab_df.reset_index(drop=True))
                all_pitch_list.append(pitch_df.reset_index(drop=True))

        all_game_df = pd.concat(all_game_list, sort=False)
        all_ab_df = pd.concat(all_ab_list, sort=False)
        all_pitch_df = pd.concat(all_pitch_list, sort=False)

        if self.as_db:
            connect = lite.connect(self.db_name + ".db")
            all_game_df.to_sql(name="games", con=connect)
            all_ab_df.to_sql(name="abs", con=connect)
            all_pitch_df.to_sql(name="pitches", con=connect)
        elif self.as_csv:
            all_game_df.to_csv(self.ds_name + "_games.csv")
            all_ab_df.to_csv(self.ds_name + "_abs.csv")
            all_pitch_df.to_csv(self.ds_name + "_pitches.csv")
        else:
            return all_game_df, all_ab_df, all_pitch_df

    def build_game_dictionary(self, game_json):

        game_data = game_json["gameData"]

        plays = game_json["liveData"]["plays"]["allPlays"]

        if len(plays) > 0 and game_data["status"]["detailedState"] == "Final":
            for val in ["winner", "loser", "firstStar", "secondStar", "thirdStar"]:
                try:
                    game_dict[val] = game_json["liveData"]["decisions"][val]["id"]
                exception Exception as e:
                    game_dict[val] = None

            game_dict = {
                "g_id_int": game_data["game"]["pk"],
                "home_team": game_data["teams"]["home"]["id"],
                "away_team": game_data["teams"]["away"]["id"],
                "stadium": game_data["venue"]["id"],
                "weather_condition": game_data["weather"].pop("condition", None),
                "temperature": game_data["weather"].pop("temp", None),
            }
            plays_by_period = [pbp_dict["plays"] for pbp_dict in game_json["liveData"]["plays"]["playsByPeriod"]]
            all_play_list = []
            for period_inds in plays_by_period:
                for temp_ind, play_ind in enumerate(period_inds):
                    play = plays[play_ind]
                    if play["result"]["eventTypeId"] not in ("PERIOD_READY", "PERIOD_START", "GAME_SCHEDULED"):
                        ab_info_dict = {}
                        ab_info_dict["g_id_int"] = game_dict["g_id_int"]
                        ab_info_dict["ab_ind"] = play["atBatIndex"]

                        for res_val in ["eventType", "description"]:
                            ab_info_dict[res_val] = play["result"].pop(res_val, None)

                        if temp_ind > 0:
                            for res_val in ["awayScore", "homeScore"]:
                                ab_info_dict[res_val] = previous_play_res[res_val]
                            ab_info_dict["outs"] = prev_outs
                        else:
                            ab_info_dict["awayScore"] = 0
                            ab_info_dict["homeScore"] = 0
                            ab_info_dict["outs"] = 0

                        if temp_ind > 0:
                            if temp_ind == 1:
                                twice_previous_runners = {"on1b": None, "on2b": None, "on3b": None}
                            else:
                                twice_previous_runners = prev_play_runners_on
                            runners_on = get_runners(previous_play_runners, twice_previous_runners)
                        else:
                            runners_on = {"on1b": None, "on2b": None, "on3b": None}
                        for key_val in list(runners_on.keys()):
                            ab_info_dict[key_val] = runners_on[key_val]

                        for abt_val in ["inning", "halfInning", "startTime", "endTime"]:
                            if "Time" in abt_val:
                                tv = play["about"].pop(abt_val, None)
                                if tv is not None:
                                    ab_info_dict[abt_val] = datetime.strptime(tv[:-5], "%Y-%m-%dT%H:%M:%S")
                                else:
                                    ab_info_dict[abt_val] = tv
                            else:
                                ab_info_dict[abt_val] = play["about"].pop(abt_val, None)

                        ab_info_dict["batter_id"] = play["matchup"]["batter"].pop("id", None)
                        ab_info_dict["batter_stance"] = play["matchup"]["batSide"].pop("code", None)
                        ab_info_dict["pitcher_id"] = play["matchup"]["pitcher"].pop("id", None)
                        ab_info_dict["pitcher_hand"] = play["matchup"]["pitchHand"].pop("code", None)

                        pitches = [ev for ev in play["playEvents"] if "call" in list(ev["details"].keys())]

                        ab_ind_tuple = (ab_info_dict["g_id_int"], ab_info_dict["ab_ind"])
                        ab_info_dict["pitches"] = get_pitch_dict(pitches, ab_ind_tuple)

                        all_ab_list.append(ab_info_dict)

                        previous_play_res = play["result"]
                        previous_play_runners = play["runners"]
                        prev_play_runners_on = runners_on
                        prev_outs = play["count"]["outs"]

            game_dict.update({"at_bats": all_ab_list})
        else:
            game_dict = None

        return game_dict

    def build_game_dataframes(self, game_json):
        game_dict = self.build_game_dictionary(game_json)

        if game_dict is not None:
            ab_list = game_dict.pop("at_bats")

            pl_temp = [ab.pop("pitches") for ab in ab_list]
            pitch_list = [p for pl in pl_temp for p in pl]

            game_ind = game_dict["g_id_int"]
            game_df = pd.DataFrame(game_dict, index=[game_ind])

            ab_ind = [ab["ab_ind"] for ab in ab_list]
            ab_df = pd.DataFrame(ab_list, index=ab_ind)

            p_ind = [p["p_ind"] for p in pitch_list]
            pitch_df = pd.DataFrame(pitch_list, index=p_ind)

            return game_df, ab_df, pitch_df
        else:
            return None, None, None

    def get_player_data(self):

        batter_dict = {}
        if self.as_db:
            cnx = lite.connect(self.db_name + ".db")
            unique_batter_query = "select distinct batter_id from abs"
            batter_df = pd.read_sql(unique_batter_query, cnx)
            for bt_id in batter_df.tolist():
                url = "https://statsapi.mlb.com/api/v1/people/" + bt_id
                id_dict = self.get_raw_url_data(url)
            pass

        return []

def main():

    g_list, a_list, p_list = [], [], []
    for season in range(2010, 2020):
        cnx = lite.connect(str(season) + "_season.db")
        g_list.append(pd.read_sql("select * from games", cnx).reset_index(drop=True))
        a_list.append(pd.read_sql("select * from abs", cnx).reset_index(drop=True))
        p_list.append(pd.read_sql("select * from pitches", cnx).reset_index(drop=True))

    all_ps = pd.concat(p_list)
    all_gs = pd.concat(g_list)
    all_as = pd.concat(a_list)

    connect = lite.connect("2010_2019_seasons.db")
    all_gs.to_sql(name="games", con=connect)
    all_as.to_sql(name="abs", con=connect)
    all_ps.to_sql(name="pitches", con=connect)

    exit()
    pfxs = MlbApiScraper(days=[1, 30], months=[2, 11], seasons=[2010, 2019], as_db=True, db_name="10_19_seasons")  # days=[10, 13], months=[8, 9], seasons=2019)

    pfxs.get_all_api_game_dfs()

    exit()

    #temp0.to_csv("temp.csv")

    temp = pfxs.get_api_game_data(565932)

    temp2 = pfxs.build_game_dataframes(temp)

    #pfxs.get_game_list_by_day(3, 1, 2019)

    #pitch_df = pfxs.get_all_game_dfs()

    '''ab_dict, use_gid = pfxs.get_game_atbat_data("gid_2017_04_14_pitmlb_chnmlb_1")

    pitch_df = pfxs.build_game_dataframe(ab_dict, use_gid)'''

    #pitch_df.to_csv("all_pitch_data.csv")

if __name__ == "__main__":

    main()


