using System;

namespace DbEngine.query
{
    public class IPL
    {
        public int id { get; set; }
        public int season { get; set; }
        public string city { get; set; }
        public DateTime date { get; set; }
        public string team1 { get; set; }
        public string team2 { get; set; }
        public string toss_winner { get; set; }
        public string toss_decision { get; set; }
        public string result { get; set; }
        public int dl_applied { get; set; }
        public string winner { get; set; }
        public int win_by_runs { get; set; }
        public int win_by_wickets { get; set; }
        public string player_of_match { get; set; }
        public string venue { get; set; }
        public string umpire1 { get; set; }
        public string umpire2 { get; set; }
        public string umpire3 { get; set; }
    }
}