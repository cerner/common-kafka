Releasing
=========

Setup / First Time
------------------

If you have never released before you will need to do the following,

 * Your user will need access to the maven central repo for our group (`com.cerner.common.kafka`)
   * Create a [JIRA account](https://issues.sonatype.org/secure/Signup!default.jspa)
   * Log a [ticket](https://issues.sonatype.org/) to get access to the repo
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

If the gpg maven plugin gives you the error `gpg: signing failed: Inappropriate ioctl for device` 
you can try doing,

```
GPG_TTY=$(tty)
export GPG_TTY
```

If the maven release plugin fails to commit things to git or create tags you can try 
the following,

`git config --add status.displayCommentPrefix true`

If the staging repository promotion command fails with 
`Unhandled: Repository: comcernercommonkafka-1001 has invalid state: open` 
this likely means the project doesn't meet some requirement of the sonatype 
repo. You can attempt to manually close the repo via the 
[staging repo](https://oss.sonatype.org) webUI which will prep it to be 
promoted. It should run the staged artifacts through a check process and 
fail for any issues. 