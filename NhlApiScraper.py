from __future__ import division
import requests
from datetime import datetime
import pandas as pd
import json
import sqlite3 as lite
import sys

class NhlApiScraper:

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
            months = [9, 6]
        elif not isinstance(months, (tuple, list)):
            months = [months]
        if days is None:
            days = [1, 30]
        elif not isinstance(days, (tuple, list)):
            days = [days]

        self.team_dict = {"njd": 1, "nyi": 2, "nyr": 3, "phi": 4, "pit": 5, "bos": 6,
                          "buf": 7, "mtl": 8, "ott": 9, "tor": 10, "car": 12, "fla": 13,
                          "tbl": 14, "wsh": 15, "chi": 16, "det": 17, "nsh": 18, "stl": 19,
                          "cgy": 20, "col": 21, "edm": 22, "van": 23, "ana": 24, "dal": 25,
                          "lak": 26, "sjs": 28, "cbj": 29, "min": 30, "wpg": 52, "ari": 53, "vgk": 54}


        if teams is None:
            teams_list = ["njd", "nyi", "nyr", "phi", "bos", "pit", "buf", "mtl", "ott",
                          "tor", "car", "fla", "tbl", "wsh", "chi", "det", "nsh", "stl",
                          "cgy", "col", "edm", "van", "ana", "dal", "lak", "sjs", "cbj",
                          "min", "ari", "wpg", "vgk"]
        else:
            teams_list = teams

        team_id_list = []
        for tm in teams_list:
            team_id_list.append(self.team_dict[tm.lower()])

        self.seasons = seasons
        self.months = months
        self.days = days
        self.teams = teams_id_list

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
            url = "https://statsapi.nhl.com/api/v1/schedule/?startDate=" + start_date + "&endDate=" + end_date

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

        all_game_list, all_play_list = [], []
        for gid in all_id_list:
            raw_data = self.get_api_game_data(gid=gid)
            df_list = self.build_game_dataframes(raw_data)

            raw_shift = self.get_shift_data(gid=gid)
            shift_list = self.build_shift_df(raw_shift)
            if not any([df is None for df in df_list]):
                game_df, play_df  = df_list
                all_game_list.append(game_df.reset_index(drop=True))
                all_play_list.append(play_df.reset_index(drop=True))

        all_game_df = pd.concat(all_game_list, sort=False)
        all_play_df = pd.concat(all_play_list, sort=False)

        if self.as_db:
            connect = lite.connect(self.db_name + ".db")
            all_game_df.to_sql(name="games", con=connect)
            all_play_df.to_sql(name="plays", con=connect)
        elif self.as_csv:
            all_game_df.to_csv(self.ds_name + "_games.csv")
            all_play_df.to_csv(self.ds_name + "_plays.csv")
        else:
            return all_game_df, all_play_df

    def build_game_dictionary(self, game_json):

        game_data = game_json["gameData"]

        plays = game_json["liveData"]["plays"]["allPlays"]

        if len(plays) > 0 and game_data["status"]["detailedState"] == "Final":
            for val in ["winner", "loser", "firstStar", "secondStar", "thirdStar"]:
                try:
                    game_dict[val] = game_json["liveData"]["decisions"][val]["id"]
                except Exception as e:
                    game_dict[val] = None

            game_dict = {
                "g_id_int": game_data["game"]["pk"],
                "home_team": game_data["teams"]["home"]["id"],
                "away_team": game_data["teams"]["away"]["id"],
                "stadium": game_data["venue"]["id"],
            }
            plays_by_period = [pbp_dict["plays"] for pbp_dict in game_json["liveData"]["plays"]["playsByPeriod"]]
            all_play_list = []
            for period_inds in plays_by_period:
                for temp_ind, play_ind in enumerate(period_inds):
                    play = plays[play_ind]
                    if play["result"]["eventTypeId"] not in ("PERIOD_READY", "PERIOD_START", "GAME_SCHEDULED", "PERIOD_OFFICIAL"):
                        play_info_dict = {}
                        play_info_dict["g_id_int"] = game_dict["g_id_int"]
                        play_info_dict["play_ind"] = play_ind

                        for res_val in ["event", "description"]:
                            play_info_dict[res_val] = play["result"].pop(res_val, None)

                        if temp_ind > 0:
                            for res_val in ["awayScore", "homeScore"]:
                                play_info_dict[res_val] = previous_play_res[res_val]
                            play_info_dict["outs"] = prev_outs
                        else:
                            play_info_dict["awayScore"] = 0
                            play_info_dict["homeScore"] = 0
                            play_info_dict["outs"] = 0


                        play_info_dict["batter_id"] = play["matchup"]["batter"].pop("id", None)
                        play_info_dict["batter_stance"] = play["matchup"]["batSide"].pop("code", None)
                        play_info_dict["pitcher_id"] = play["matchup"]["pitcher"].pop("id", None)
                        play_info_dict["pitcher_hand"] = play["matchup"]["pitchHand"].pop("code", None)

                        pitches = [ev for ev in play["playEvents"] if "call" in list(ev["details"].keys())]

                        ab_ind_tuple = (play_info_dict["g_id_int"], play_info_dict["ab_ind"])
                        play_info_dict["pitches"] = get_pitch_dict(pitches, ab_ind_tuple)

                        all_ab_list.append(play_info_dict)

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
            play_list = game_dict.pop("plays")

            game_ind = game_dict["g_id_int"]
            game_df = pd.DataFrame(game_dict, index=[game_ind])

            play_ind = [play["play_ind"] for play in play_list]
            play_df = pd.DataFrame(play_list, index=play_ind)

            return game_df, play_df
        else:
            return None, None

    def get_shift_data(self, gid):

        url = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=" + str(gid)

        raw = self.get_raw_url_data(url)

        raw_json = json.loads(raw)

        return raw_json

    def build_shift_df(self, shift_json)

        return shift_df

    def get_player_data(self):

        player_dict = {}
        if self.as_db:
            cnx = lite.connect(self.db_name + ".db")
            unique_player_query = "select distinct player_id from plays"
            player_df = pd.read_sql(unique_player_query, cnx)
            for plyr_id in player_df.tolist():
                url = "https://statsapi.nhl.com/api/v1/people/" + plyr_id
                id_dict = self.get_raw_url_data(url)
            pass

        return []

def main():

    nhls = NhlApiScraper(days=[1, 30], months=[10, 6], seasons=[2010, 2019], as_db=True, db_name="10_19_seasons")  # days=[10, 13], months=[8, 9], seasons=2019)

    nhls.get_all_api_game_dfs()

if __name__ == "__main__":

    main()



