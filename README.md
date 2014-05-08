github-data-tools
=================

## Description

A handy set of tools which first gets data from GitHub Archive website. Secondly, we unpack that data and add .json files to a mongodb database. Finally, we analyze those GitHub Events to create list of developers and their attributes which are important for typical OSS and COIN analysis.

### Requirements

* Python with all installed packages
* Mongodb with GHA events or at least downloaded GHA (import first)

### Dimensionsal output data

Dimensions which we want to describe for a developer:

1.	Number of following; [FollowEvent]
2.	Number of followers; [FollowEvent]
3.	How many developers in projects created by him [PushEvent] [IssuesEvent] [PullRequestEvent] [GollumEvent]
4.	How many collaborators in projects created by him [TeamAddEvent] [MemberEvent]
5.	In how many repos, not created by him, he is a collaborator [TeamAddEvent] [MemberEvent]
6.	In how many repos, not created by him, he is a contributor [PushEvent] [IssuesEvent] [PullRequestEvent] [GollumEvent]
7.	Code quality globally and in a repo [apart from GHA]
8.	Time spent in a repo [PushEvent]
9.	Number of commits per skill [PushEvent]
  9.	number of commits globally
  9.	number of commits in a repo
  9.	ratio for i/ii
10.	Number of discussions in a repo [CommitCommentEvent] [IssueCommentEvent] [PullRequestReviewCommentEvent]
11.	Number of closed 'feature issues' by him globally [IssuesEvent]
12.	Number of closed 'bug issues' by him globally [IssuesEvent]
13.	Time from the first commit [PushEvent]
14.	Time from the last commit  [PushEvent]
15.	Time between commits [PushEvent]
16.	Usuall time of commiting [PushEvent]

Find out more at:

http://wikiteams.github.io/github-data-tools
