Releasing
=========

Setup / First Time
------------------

If you have never released before you will need to do the following,

 * Your user will need access to the maven central repo for our group (`com.cerner.common.kafka`)
   * Create a [JIRA account](https://issues.sonatype.org/secure/Signup!default.jspa)
   * Log a ticket ([like this one](https://issues.sonatype.org/browse/OSSRH-37290)) to get access to the repo. You will need one of the owners of the project to approve the JIRA
 * Install gpg (for Mac `brew install gnupg`)
 * Setup gpg key (http://central.sonatype.org/pages/working-with-pgp-signatures.html)
   * Create new key `gpg --gen-key`. Follow instructions
   * Share public key `gpg --keyserver pgp.mit.edu --send-keys KEY_ID`
 * Add the following to `~/.m2/settings.xml`

```
<settings>
  <servers>
    <server>
      <id>ossrh</id>
      <username>JIRA_USER</username>
      <password>JIRA_PASSWORD</password>
    </server>
  </servers>
</settings>
```

 * You can also setup your gpg passphrase into `settings.xml` following [this](http://central.sonatype.org/pages/apache-maven.html#gpg-signed-components) but I was unable to get it to work

Releasing the Project
---------------------

If you've done the setup to release the project do the following,

`mvn release:clean release:prepare release:perform -P ossrh`

This will,

 * Drop `-SNAPSHOT` from version
 * Create a git tag
 * Bump version and add `-SNAPSHOT` for next development
 * Push artifact to [staging](https://oss.sonatype.org)

At this point you can check the artifacts if you would like in the 
[staging repo](https://oss.sonatype.org). If everything looks good you can then do,

`mvn nexus-staging:release -P ossrh -DstagingRepositoryId=REPO_ID`

This promotes the artifacts from staging to public maven central.

You can get the `REPO_ID` either look for,  
`Closing staging repository with ID "comcernercommonkafka-1002"` 
in the maven logs or from the [staging repo](https://oss.sonatype.org). 

### Common Issues

#### gpg: signing failed: Inappropriate ioctl for device

If the gpg maven plugin gives you the error `gpg: signing failed: Inappropriate ioctl for device` 
you can try doing,

```
GPG_TTY=$(tty)
export GPG_TTY
```

#### Maven Fails to Commit/Push

If the maven release plugin fails to commit things to git or create tags you can try 
the following,

`git config --add status.displayCommentPrefix true`

#### Unhandled: Repository: comcernercommonkafka-XXXX has invalid state: open

If the staging repository promotion command fails with 
`Unhandled: Repository: comcernercommonkafka-1001 has invalid state: open` 
this likely means the project doesn't meet some requirement of the sonatype 
repo. You can attempt to manually close the repo via the 
[staging repo](https://oss.sonatype.org) webUI which will prep it to be 
promoted. It should run the staged artifacts through a check process and 
fail for any issues. 

#### Rule failure: No public key: Key with id: XXXXXX

This means that one of the sonatype repository rules about having a valid GPG key 
failed and they were unable to find your key in any of the public key servers.

Pushing your gpg key to a public key server should likely fix the issue,

```
gpg --send-key KEY_ID
```

Then verify with,

```
gpg --recv-key KEY_ID
```

If this isn't working you can try manually uploading,

```
gpg --armor --export KEY_ID
```

Then going to [https://pgp.mit.edu/](https://pgp.mit.edu/) and uploading the key
into the UI.

Another option I've found that worked when the others didn't was to use another 
keyserver,

```
gpg --keyserver hkp://keyserver.ubuntu.com --send-key KEY_ID
```

And verify,

``` 
gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys KEY_ID
```

Additionally this can still fail even if the key is found on the key server if 
your key is actually a sub key. You can see this with,

``` 
gpg --list-keys 
```

If your key contains the `sub` section. If it does you likely need to delete your 
key(s),

``` 
gpg --delete-secret-key KEY_ID 
gpg --delete-key KEY_ID
```

Then generate a new key that is a 'sign only' key,

``` 
gpg --full-generate-key
```

Make sure to select the 'Sign Only' and 'RSA' options. Then follow the same options 
above to upload your key and verify its on the public key servers and try the 
release again.