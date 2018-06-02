package models

import (
	"github.com/jinzhu/gorm"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/http"
	"os"
	"path"
	"time"
)

type Repository struct {
	Model
	Name              string     `json:"name"`
	RemoteURL         string     `json:"remote_url"`
	AuthUser          string     `json:"auth_user"`
	LocalPath         string     `json:"-"`
	LastCommitHash    string     `json:"last_commit_hash"`
	LastCommitMessage string     `json:"last_commit_message"`
	LastCommitDate    *time.Time `json:"last_commit_date"`
	LastCommitAuthor  string     `json:"last_commit_author"`
}

func (r *Repository) Create(db *gorm.DB) error {
	return db.Create(r).Error
}

func (r *Repository) Update(db *gorm.DB) error {
	return db.Save(r).Error
}

func (r *Repository) Delete(db *gorm.DB) error {
	err := db.Delete(r).Error
	if err != nil {
		return err
	}
	return os.RemoveAll(r.LocalPath)
}

func (r *Repository) Pull(password string) error {
	rr, err := git.PlainOpen(r.LocalPath)
	if err != nil {
		return err
	}
	w, err := rr.Worktree()
	if err != nil {
		return err
	}
	return w.Pull(&git.PullOptions{
		RemoteName:        "origin",
		Force:             true,
		SingleBranch:      true,
		RecurseSubmodules: git.DefaultSubmoduleRecursionDepth,
		Auth:              &http.BasicAuth{r.AuthUser, password},
	})
}

func (r *Repository) UpdateStats(db *gorm.DB) error {
	rr, err := git.PlainOpen(r.LocalPath)
	if err != nil {
		return err
	}
	ref, err := rr.Head()
	if err != nil {
		return err
	}
	cIter, err := rr.Log(&git.LogOptions{From: ref.Hash()})
	if err != nil {
		return err
	}
	commit, err := cIter.Next()
	if err != nil {
		return err
	}
	r.LastCommitAuthor = commit.Author.Name
	r.LastCommitMessage = commit.Message
	r.LastCommitDate = &commit.Author.When
	r.LastCommitHash = commit.Hash.String()
	return r.Update(db)
}

func repoName(url string) string {
	var i int
	if url[len(url)-1] == '/' {
		//if the url ends with trailing slash, look for last non-empty segment
		i = len(url) - 2
	} else {
		//URL doesn't end with slash - get last segment
		i = len(url) - 1
	}
	for ; i > 0; i-- {
		if url[i] == '/' {
			break
		}
	}
	if url[i] != '/' {
		return url //not a valid URL
	}
	if url[len(url)-1] == '/' {
		return url[i+1 : len(url)-1]
	}
	return url[i+1:]
}

func (r *Repository) Clone(db *gorm.DB, dir, password string) error {
	info, err := git.PlainClone(path.Join(dir, repoName(r.RemoteURL)), false, &git.CloneOptions{
		URL:  r.RemoteURL,
		Auth: &http.BasicAuth{r.AuthUser, password},
	})
	if err != nil {
		return err
	}
	w, err := info.Worktree()
	if err != nil {
		return err
	}
	r.LocalPath = w.Filesystem.Root()
	return r.Update(db)
}
