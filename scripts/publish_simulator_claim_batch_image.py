"""ADR0368 publish only an actually qualified immutable local candidate."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

from prepare_shared_application_image import ROOT
from prepare_simulator_claim_batch_image import VERIFY

REPOSITORY='swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing'


def run(*args):
    return subprocess.run(['docker',*args],check=True,text=True,capture_output=True,timeout=300)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image-receipt',type=Path,required=True)
    parser.add_argument('--qualification',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--verify-existing')
    args=parser.parse_args()
    if args.output.exists() or not args.output.resolve().is_relative_to((ROOT/'docs/capacity/cce').resolve()):
        raise ValueError('Fresh public publication receipt required')
    image,quality=[json.loads(p.read_text(encoding='utf-8')) for p in (args.image_receipt,args.qualification)]
    if (image.get('decision')!='ADR0368' or not image.get('built') or image.get('registry_published')
            or quality.get('integration_pass') is not True or quality.get('cleanup_pass') is not True
            or quality.get('source_identity_sha256')!=image['source_identity_sha256']
            or quality.get('local_image_id')!=image['local_image_id']):
        raise ValueError('Executed exact-image correctness and local cleanup required')
    for name,key in [('prepare_simulator_claim_batch_image.py','preparation_recipe_sha256'),
                     ('simulator_claim_batch_overlay.py','overlay_sha256')]:
        if hashlib.sha256((ROOT/'scripts'/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()!=image[key]:
            raise ValueError('Qualified image preparation changed')
    if json.loads(run('image','inspect',image['local_image_id']).stdout)[0]['Id']!=image['local_image_id']:
        raise ValueError('Local candidate changed')
    tag=REPOSITORY+':adr0368-'+image['source_identity_sha256'][:12]
    log=ROOT/'tmp/adr0368-push.private.log'
    if args.verify_existing:
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',args.verify_existing) or not log.is_file() or log.is_symlink():
            raise ValueError('Exact retained push identity required')
        output=log.read_text(encoding='utf-8')
    else:
        run('tag',image['local_image_id'],tag)
        pushed=run('push',tag);output=pushed.stdout+pushed.stderr
        log.write_text(output,encoding='utf-8')
    matches=re.findall(r'digest:\s+(sha256:[0-9a-f]{64})',output)
    if not matches or (args.verify_existing and matches[-1]!=args.verify_existing):
        raise ValueError('Actual immutable registry digest required')
    digest=matches[-1];immutable=REPOSITORY+'@'+digest
    manifest=json.loads(run('manifest','inspect',immutable).stdout)
    run('pull',immutable)
    pulled=json.loads(run('image','inspect',immutable).stdout)[0]
    verified=json.loads(run('run','--rm','--network=none','--entrypoint','python',immutable,'-c',VERIFY).stdout)
    if (immutable not in pulled['RepoDigests'] or pulled['Id']!=image['local_image_id']
            or (pulled['Os'],pulled['Architecture'],pulled['Config']['User'])!=('linux','amd64','ticketing')
            or verified!={'runtime_modules_verified':22,'default_off':True,'claim_limit':16,'installed_module_matches':True}):
        raise ValueError('Published and pulled runtime differs')
    result={k:image[k] for k in ('parent_source_identity_sha256','parent_runtime_source_sha256',
        'runtime_source_sha256','source_identity_sha256','dependency_input_sha256','changed_modules',
        'preparation_recipe_sha256','overlay_sha256','local_image_id')}
    result.update(decision='ADR0368',registry_image=immutable,registry_manifest_digest=digest,
        registry_configuration_digest=manifest['config']['digest'],registry_pulled_image_id=pulled['Id'],
        registry_published=True,permission_verified=True,registry_pulled_runtime_verified=True,
        validation=verified,platform={'os':'linux','architecture':'amd64'},runtime_user='ticketing',
        cloud_load_started=False,capacity_improvement_measured=False)
    args.output.write_bytes((json.dumps(result,indent=2)+'\n').encode())
    print(json.dumps({'published':True,'manifest':digest,'configuration':manifest['config']['digest'],
                      'source':image['source_identity_sha256'],'pulled_modules_verified':22,'cloud_traffic_started':False}))


if __name__=='__main__':main()
