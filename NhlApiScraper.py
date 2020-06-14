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
        self.teams = team_id_list

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
                url = "https://statsapi.web.nhl.com/api/v1/schedule/?startDate=" + sd + "&endDate=" + ed

                raw = self.get_raw_url_data(url)

                raw_json = json.loads(raw)

                json_list.append(raw_json)
        else:
            url = "https://statsapi.web.nhl.com/api/v1/schedule/?startDate=" + start_date + "&endDate=" + end_date

            raw = self.get_raw_url_data(url)

            raw_json = json.loads(raw)

            json_list = [raw_json]

        return json_list

    def get_api_game_data(self, gid):

        url = "https://statsapi.web.nhl.com/api/v1/game/" + str(gid) + "/feed/live"

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
            raw_shift = self.get_shift_data(gid=gid)
            shift_dict = self.build_shift_dict(raw_shift)

            df_list = self.build_game_dataframes(raw_data, shift_dict)

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

    def build_game_dictionary(self, game_json, shift_dict):

        game_data = game_json["gameData"]

        plays = game_json["liveData"]["plays"]["allPlays"]

        if len(plays) > 0 and game_data["status"]["detailedState"] == "Final":

            homeId = game_data["teams"]["home"]["id"]
            awayId = game_data["teams"]["away"]["id"]

            game_dict = {
                "g_id_int": game_data["game"]["pk"],
                "home_team": homeId,
                "away_team": awayId,
                "stadium": game_data["venue"]["id"],
            }

            for val in ["winner", "loser", "firstStar", "secondStar", "thirdStar"]:
                try:
                    game_dict[val] = game_json["liveData"]["decisions"][val]["id"]
                except Exception as e:
                    game_dict[val] = None

            all_play_list = []
            previous_play_goals = None
            for play_ind, play in enumerate(plays):
                if play["result"]["eventTypeId"] not in ("PERIOD_READY", "PERIOD_START", "GAME_SCHEDULED", "PERIOD_OFFICIAL", "GAME_END", "GAME_OFFICIAL"):
                    play_info_dict = {}
                    play_info_dict["g_id_int"] = game_dict["g_id_int"]
                    play_info_dict["play_ind"] = play_ind

                    for res_val in ["eventTypeId", "secondaryType", "description"]:
                        play_info_dict[res_val] = play["result"].pop(res_val, None)

                    for coor_val in ["x", "y"]:
                        play_info_dict[coor_val] = play["coordinates"].pop(coor_val, None)

                    for p_val in range(4):
                        try:
                            try:
                                play_info_dict["player" + str(p_val)] = play["players"][p_val]["player"]["id"]
                                play_info_dict["player" + str(p_val) + "Type"] = play["players"][p_val]["playerType"]
                            except IndexError:
                                play_info_dict["player" + str(p_val)] = None
                                play_info_dict["player" + str(p_val) + "Type"] = None
                        except KeyError:
                            play_info_dict["player" + str(p_val)] = None
                            play_info_dict["player" + str(p_val) + "Type"] = None

                    if previous_play_goals is not None:
                        for res_val in ["away", "home"]:
                            play_info_dict[res_val + "Goals"] = previous_play_goals[res_val]
                    else:
                        play_info_dict["awayGoals"] = 0
                        play_info_dict["homeGoals"] = 0

                    period = play["about"]["period"]
                    time = play["about"]["periodTime"]

                    play_info_dict["period"] = period
                    play_info_dict["periodTime"] = time

                    on_ice = self.get_shift(shift_dict, period, time, homeId, awayId)

                    play_info_dict.update(on_ice)

                    all_play_list.append(play_info_dict)

                    previous_play_goals = play["about"]["goals"]

            game_dict.update({"plays": all_play_list})
        else:
            game_dict = None

        return game_dict

    def build_game_dataframes(self, game_json, shift_dict):
        game_dict = self.build_game_dictionary(game_json, shift_dict)

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

        return raw_json["data"]

    def build_shift_dict(self, shift_json):

        shift_dict = {}
        for shift in shift_json:
            temp_dict = {}
            for value in ["startTime", "endTime", "teamId", "period"]:
                temp_dict[value] = shift.pop(value, None)
            temp_dict["intStartTime"] = (20*60)*(temp_dict["period"] - 1) + 60*int(temp_dict["startTime"].split(":")[0]) + int(temp_dict["startTime"].split(":")[-1])
            temp_dict["intEndTime"] = (20*60)*(temp_dict["period"] - 1) + 60*int(temp_dict["endTime"].split(":")[0]) + int(temp_dict["endTime"].split(":")[-1])
            shift_dict[(shift["playerId"], shift["shiftNumber"])] = temp_dict

        minTime = 0
        maxTime = max([d["intEndTime"] for d in shift_dict.values()])

        team0 = min([d["teamId"] for d in shift_dict.values()])
        team1 = max([d["teamId"] for d in shift_dict.values()])

        time_list = range(minTime, maxTime + 1)

        all_time_dict = {}
        for t in time_list:
            t0_list = []
            t1_list = []
            for p, s in shift_dict.items():
                if t in range(s["intStartTime"], s["intEndTime"]):
                    if s["teamId"] == team0:
                        t0_list.append(p)
                    else:
                        t1_list.append(p)
            all_time_dict[t] = {team0: t0_list, team1: t1_list}

        return all_time_dict

    def get_shift(self, shift_dict, period, time, hId, aId):

        timeInt = 20*60*(period-1) + 60*int(time.split(":")[0]) + int(time.split(":")[-1])

        onIce = shift_dict[timeInt]

        home_list = onIce[hId]
        away_list = onIce[aId]

        on_ice_at_time_dict = {}
        for i in range(6):
            try:
                on_ice_at_time_dict["on_ice_home_p" + str(i + 1)] = home_list[i][0]
                on_ice_at_time_dict["on_ice_home_p" + str(i + 1) + "_shift"] = home_list[i][-1]
            except IndexError:
                on_ice_at_time_dict["on_ice_home_p" + str(i + 1)] = -1
                on_ice_at_time_dict["on_ice_home_p" + str(i + 1) + "_shift"] = -1
            try:
                on_ice_at_time_dict["on_ice_away_p" + str(i + 1)] = away_list[i][0]
                on_ice_at_time_dict["on_ice_away_p" + str(i + 1) + "_shift"] = away_list[i][-1]
            except IndexError:
                on_ice_at_time_dict["on_ice_away_p" + str(i + 1)] = -1
                on_ice_at_time_dict["on_ice_away_p" + str(i + 1) + "_shift"] = -1

        return on_ice_at_time_dict

    def get_player_data(self):

        player_dict = {}
        if self.as_db:
            cnx = lite.connect(self.db_name + ".db")
            unique_player_query = "select distinct player_id from plays"
            player_df = pd.read_sql(unique_player_query, cnx)
            for plyr_id in player_df.tolist():
                url = "https://statsapi.web.nhl.com/api/v1/people/" + plyr_id
                id_dict = self.get_raw_url_data(url)
            pass

        return []

def main():

    nhls = NhlApiScraper(days=[1, 30], months=[10, 6], seasons=[2018, 2019], as_db=True, db_name="10_19_seasons")  # days=[10, 13], months=[8, 9], seasons=2019)

    nhls.get_all_api_game_dfs()

if __name__ == "__main__":

    main()



